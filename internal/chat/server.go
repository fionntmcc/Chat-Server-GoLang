package chat

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

const (
	// defaultRoom is where every client lands on connecting.
	defaultRoom = "general"

	// outboundBuffer is how many messages a client may fall behind by before it
	// is disconnected. This is the backpressure budget: large enough to absorb a
	// brief stall, small enough that a dead-but-open connection cannot consume
	// unbounded memory.
	outboundBuffer = 64

	// broadcastBuffer is how many messages may be queued for the hub before
	// senders start waiting on it.
	broadcastBuffer = 64

	// historyLimit is how many past messages a client receives on joining a room.
	historyLimit = 50
)

func presence(username, room, what string) []byte {
	return ToByteArray(Message{Username: username, Text: what, Room: room, CreatedAt: time.Now()})
}

// signal reports completion by closing rather than sending. close cannot block;
// a send would wedge the hub permanently if the waiting goroutine had given up.
// The hub must never block on anything a client controls.
func signal(reply chan struct{}) {
	if reply != nil {
		close(reply)
	}
}

// answer delivers a result. reply must be buffered so this cannot block either.
func answer(reply chan error, err error) {
	if reply != nil {
		reply <- err
	}
}

// --- server -----------------------------------------------------------------

type Server struct {
	listener net.Listener
	store    *Store
	hub      *hub
	quit     chan struct{}
}

func NewServer(address string, dbPath string) (*Server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %v", address, err)
	}

	store, err := NewStore(dbPath)
	if err != nil {
		listener.Close()
		return nil, fmt.Errorf("failed to open store: %v", err)
	}

	quit := make(chan struct{})

	return &Server{
		listener: listener,
		store:    store,
		hub:      newHub(quit),
		quit:     quit,
	}, nil
}

// Addr returns the address the listener is bound to. Tests bind to port 0 and
// need the port the OS actually assigned.
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

func (s *Server) Start() {
	go s.hub.run()

	log.Printf("server listening on %s", s.listener.Addr())

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}
		go s.handleConn(conn)
	}
}

func (s *Server) Shutdown() {
	// Closing quit stops the accept loop, stops the hub, and releases any
	// handleConn goroutine blocked sending to it. The hub disconnects the clients
	// it owns as it exits.
	close(s.quit)
	s.listener.Close()
	s.store.Close()
}

// enter registers c in room and waits for the hub to confirm, so history replay
// and the banner cannot be processed ahead of the join.
func (s *Server) enter(c *Client, room string) bool {
	reply := make(chan struct{})
	if !s.hub.send(s.hub.register, membership{client: c, room: room, reply: reply}) {
		return false
	}
	select {
	case <-reply:
		return true
	case <-s.quit:
		return false
	}
}

// exit removes c from room and reports whether the hub confirmed it.
//
// The return value is load-bearing. c.out has two senders — the hub's deliver and
// handleConn's queue — and closing a channel with more than one sender is only
// safe if every other sender is known to have stopped. Confirmation from the hub
// is that proof: the hub is single-threaded, so having answered means it is no
// longer inside deliver for this client and c is in no room's member set.
//
// If confirmation does not arrive (the server is shutting down), the caller must
// NOT close c.out. The hub kicks the clients it owns as it exits, and kick shares
// closeOnce with closeGracefully, so teardown still happens exactly once.
//
// The old mutex version got this ordering for free: the write lock needed to
// delete from s.clients could not be acquired while broadcastLoop held the read
// lock. Swapping a lock for a channel does not preserve that guarantee — it has
// to be re-established explicitly.
func (s *Server) exit(c *Client, room string) bool {
	reply := make(chan struct{})
	if !s.hub.send(s.hub.unregister, membership{client: c, room: room, reply: reply}) {
		return false
	}
	select {
	case <-reply:
		return true
	case <-s.quit:
		return false
	}
}

// move relocates c and waits for the hub to finish the whole transition.
func (s *Server) move(c *Client, from, to string) bool {
	reply := make(chan error, 1) // buffered: the hub must not block answering
	if !s.hub.sendSwitch(roomChange{client: c, from: from, to: to, reply: reply}) {
		return false
	}
	select {
	case err := <-reply:
		if err != nil {
			c.queuef("cannot join #%s: %v\n", to, err)
			return false
		}
		return true
	case <-s.quit:
		return false
	}
}

// list asks the hub for a snapshot and waits for it. Returns false if the server
// is shutting down.
func (s *Server) list(ch chan listRequest, room string) ([]string, bool) {
	reply := make(chan []string, 1) // buffered: the hub must not block answering
	if !s.hub.sendList(ch, listRequest{room: room, reply: reply}) {
		return nil, false
	}
	select {
	case names := <-reply:
		return names, true
	case <-s.quit:
		return nil, false
	}
}

// replayHistory queues the tail of room to c alone.
func (s *Server) replayHistory(c *Client, room string) {
	history, err := s.store.GetMessages(room, historyLimit)
	if err != nil {
		log.Printf("error loading history for #%s: %v", room, err)
		return
	}
	for _, m := range history {
		c.queue(ToByteArray(m))
	}
}

func (s *Server) handleConn(conn net.Conn) {
	// The client exists as soon as the socket does. out and done are meaningful
	// before any identity is known, so writeLoop starts immediately and every
	// byte the server sends — prompts and handshake errors included — travels
	// through out. That gives conn exactly one writer.
	client := &Client{
		conn: conn,
		out:  make(chan []byte, outboundBuffer),
		done: make(chan struct{}),
	}
	go client.writeLoop()

	scanner := bufio.NewScanner(conn)

	// --- handshake: constructed but not yet registered ---

	client.queuef("Enter username: \n")
	if !scanner.Scan() {
		client.closeGracefully()
		return
	}

	username := strings.TrimSpace(scanner.Text())
	if username == "" {
		client.queuef("username cannot be empty\n")
		// Graceful, not kick: the client must actually receive the reason. A bare
		// conn.Close() here would discard the queued message.
		client.closeGracefully()
		return
	}
	client.username = username

	// room is this goroutine's own copy. It is never shared, so there is nothing
	// to synchronise; the hub's map is the source of truth for delivery.
	room := defaultRoom

	// The banner is queued before registration so that it is the first line a
	// client sees after the prompt. That matters to more than aesthetics: the
	// test harness treats the banner as the "registration complete" signal, and
	// anything queued ahead of it would be consumed while waiting for it.
	client.queuef("You are in #%s. Use /join <room> to switch rooms.\n", room)

	// --- registration: from here the client is reachable by broadcasts ---

	if !s.enter(client, room) {
		client.closeGracefully()
		return
	}

	defer func() {
		// room is read from the enclosing scope, so this sees the current room
		// even after a /join.
		if s.exit(client, room) {
			// Confirmed removal: the hub will not send again, and this goroutine
			// is the only other sender, so closing out is safe and lets writeLoop
			// flush whatever is still queued.
			client.closeGracefully()
		}
		// Otherwise the server is shutting down and the hub kicks its clients as
		// it exits. Closing out here would race with an in-flight deliver.
	}()

	// History comes after registration, not before, so a message sent by someone
	// else during replay is queued behind the backlog rather than lost.
	s.replayHistory(client, room)

	// --- message loop ---

	for scanner.Scan() {
		text := scanner.Text()

		// Dispatch on the command word rather than a prefix: HasPrefix(text,
		// "/who") also matches "/whoever". Phase 7.2 replaces this switch with a
		// map[string]handlerFunc; it is already close to the size that justifies
		// the change.
		command, arg, _ := strings.Cut(text, " ")
		arg = strings.TrimSpace(arg)

		switch command {
		case "/join":
			if arg == "" {
				client.queuef("Usage: /join <room>\n")
				continue
			}
			if arg == room {
				client.queuef("You are already in #%s\n", room)
				continue
			}

			// The hub emits both presence announcements as part of the move.
			if !s.move(client, room, arg) {
				return
			}
			room = arg

			s.replayHistory(client, room)
			client.queuef("Switched to #%s\n", room)
			continue

		case "/who":
			names, ok := s.list(s.hub.who, room)
			if !ok {
				return
			}
			client.queuef("#%s (%d): %s\n", room, len(names), strings.Join(names, ", "))
			continue

		case "/rooms":
			names, ok := s.list(s.hub.rooms, "")
			if !ok {
				return
			}
			client.queuef("rooms (%d): %s\n", len(names), strings.Join(names, ", "))
			continue
		}

		// Regular message — persist, then broadcast.
		if _, err := s.store.SaveMessage(room, username, text); err != nil {
			log.Printf("error saving message: %v", err)
		}
		msg := Message{Username: username, Text: text, Room: room, CreatedAt: time.Now()}
		if !s.hub.sendBroadcast(envelope{room: room, data: ToByteArray(msg)}) {
			return
		}
	}
}
