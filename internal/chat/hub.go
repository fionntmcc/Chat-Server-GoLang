package chat

import (
	"sort"
)


// --- hub --------------------------------------------------------------------
//
// The hub goroutine is the sole owner of room membership. Every mutation arrives
// as a message on one of its channels, which is what replaces s.mu: there is no
// lock because there is no sharing.

type envelope struct {
	room string
	data []byte
}

// membership is used for both register and unregister. It carries the room
// because Client no longer has a room field — the hub's map is the only source
// of truth, and handleConn keeps its own local copy for message construction.
type membership struct {
	client *Client
	room   string
	reply  chan struct{}
}

type roomChange struct {
	client *Client
	from   string
	to     string
	reply  chan error
}

// listRequest asks the hub for a snapshot of state it owns. reply must be
// buffered so the hub cannot block answering a caller that has gone away.
type listRequest struct {
	room  string // for who; ignored by rooms
	reply chan []string
}

type hub struct {
	register   chan membership
	unregister chan membership
	switchRoom chan roomChange
	broadcast  chan envelope
	who        chan listRequest
	rooms      chan listRequest
	quit       chan struct{}
}

func newHub(quit chan struct{}) *hub {
	return &hub{
		// Unbuffered: a send completes only once the hub has taken the value, and
		// the hub finishes handling it before selecting again. That ordering is
		// what lets handleConn rely on being registered before anything else it
		// sends can be processed.
		register:   make(chan membership),
		unregister: make(chan membership),
		switchRoom: make(chan roomChange),
		broadcast:  make(chan envelope, broadcastBuffer),
		who:        make(chan listRequest),
		rooms:      make(chan listRequest),
		quit:       quit,
	}
}

func (h *hub) run() {
	// rooms is a local, not a struct field. A field could be reached from
	// anywhere; a local can only be touched by this goroutine. That distinction
	// is the entire mechanism replacing the mutex.
	rooms := map[string]map[*Client]struct{}{}

	for {
		select {
		case <-h.quit:
			// The owner of the registry is also responsible for tearing it down.
			for _, members := range rooms {
				for c := range members {
					c.kick("server shutting down")
				}
			}
			return

		case m := <-h.register:
			joinRoom(rooms, m.room, m.client)
			// Announce after adding so the joiner sees its own arrival.
			deliver(rooms[m.room], presence(m.client.username, m.room, "has joined"))
			signal(m.reply)

		case m := <-h.unregister:
			leaveRoom(rooms, m.room, m.client)
			deliver(rooms[m.room], presence(m.client.username, m.room, "has left"))
			signal(m.reply)

		case rc := <-h.switchRoom:
			// The whole transition runs here, in order, in one goroutine.
			// Routing it through a channel is not sufficient on its own: select
			// picks randomly among ready cases, so a leave announcement sent
			// separately on h.broadcast could be processed after the move and
			// delivered to the wrong room. Owning the sequence is the fix.
			leaveRoom(rooms, rc.from, rc.client)
			deliver(rooms[rc.from], presence(rc.client.username, rc.from, "has left"))

			joinRoom(rooms, rc.to, rc.client)
			deliver(rooms[rc.to], presence(rc.client.username, rc.to, "has joined"))

			answer(rc.reply, nil)

		case e := <-h.broadcast:
			// A missing room yields a nil map, and ranging a nil map is a no-op.
			deliver(rooms[e.room], e.data)

		// Reads of hub-owned state are requests too. handleConn cannot inspect
		// rooms directly — that is the point of the hub — so it asks and the hub
		// answers with a snapshot.
		case q := <-h.who:
			members := rooms[q.room]
			names := make([]string, 0, len(members))
			for c := range members {
				names = append(names, c.username)
			}
			// Map iteration order is randomised, so sort or the same room returns
			// a different order every call and any test on it is flaky.
			sort.Strings(names)
			q.reply <- names

		case q := <-h.rooms:
			names := make([]string, 0, len(rooms))
			for room := range rooms {
				names = append(names, room)
			}
			sort.Strings(names)
			q.reply <- names
		}
	}
}

// Every send into the hub also watches quit. Without that, a handleConn goroutine
// would block forever once run has returned — a goroutine leak, and one goleak
// will fail on in Phase 3.

func (h *hub) send(ch chan membership, m membership) bool {
	select {
	case ch <- m:
		return true
	case <-h.quit:
		return false
	}
}

func (h *hub) sendSwitch(rc roomChange) bool {
	select {
	case h.switchRoom <- rc:
		return true
	case <-h.quit:
		return false
	}
}

func (h *hub) sendBroadcast(e envelope) bool {
	select {
	case h.broadcast <- e:
		return true
	case <-h.quit:
		return false
	}
}

func (h *hub) sendList(ch chan listRequest, q listRequest) bool {
	select {
	case ch <- q:
		return true
	case <-h.quit:
		return false
	}
}