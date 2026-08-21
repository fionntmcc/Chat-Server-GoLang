package chat

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	// outboundBuffer is how many messages a client may fall behind by before it
	// is disconnected. This is the backpressure budget: large enough to absorb a
	// brief stall, small enough that a dead-but-open connection cannot consume
	// unbounded memory.
	outboundBuffer = 64

	// historyLimit is how many past messages a client receives on joining a room.
	historyLimit = 50
)

type Client struct {
	conn      net.Conn
	username  string
	room      string
	out       chan []byte // buffered
	done      chan struct{}
	closeOnce sync.Once
}

// queue hands b to writeLoop. It never blocks: if the outbound buffer is full,
// the client is disconnected rather than allowed to stall the caller. That is
// the same policy broadcastLoop applies, and it is the whole point of 2.1 — no
// code path may block on a slow socket.
func (c *Client) queue(b []byte) {
	select {
	case c.out <- b:
	default:
		c.kick("outbound buffer full")
	}
}

// queuef is queue for server-generated notices addressed to one client.
func (c *Client) queuef(format string, args ...any) {
	c.queue([]byte(fmt.Sprintf(format, args...)))
}

func (c *Client) writeLoop() {
	defer c.conn.Close()

	for {
		select {
		case <-c.done:
			return
		case b, ok := <-c.out:
			if !ok {
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := c.conn.Write(b); err != nil {
				return
			}
		}
	}
}

// kick disconnects the client immediately, abandoning anything still queued.
// Use it when the queue itself is the problem.
func (c *Client) kick(reason string) {
	c.closeOnce.Do(func() {
		log.Printf("disconnecting %s: %s", c.username, reason)
		close(c.done)
		c.conn.Close()
	})
}

// closeGracefully lets writeLoop drain whatever is queued before closing the
// connection. Use it for validation errors and normal disconnects, where the
// last message the client receives is the one that explains what happened.
//
// It shares closeOnce with kick, so whichever runs first wins and neither can
// close a channel twice.
func (c *Client) closeGracefully() {
	c.closeOnce.Do(func() {
		close(c.out)
	})
}

type Server struct {
	listener  net.Listener
	store     *Store
	clients   map[net.Conn]*Client
	mu        sync.RWMutex
	broadcast chan BroadcastMsg
	quit      chan struct{}
}

type BroadcastMsg struct {
	room string
	data []byte
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

	return &Server{
		listener:  listener,
		store:     store,
		clients:   make(map[net.Conn]*Client),
		broadcast: make(chan BroadcastMsg, 64),
		quit:      make(chan struct{}),
	}, nil
}

func (s *Server) Start() {
	go s.broadcastLoop()

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
	close(s.quit)
	s.listener.Close()

	s.mu.Lock()
	for conn := range s.clients {
		conn.Close()
	}
	s.mu.Unlock()

	s.store.Close()
}

func (s *Server) broadcastLoop() {
	for {
		select {
		case <-s.quit:
			return
		case msg := <-s.broadcast:
			var slow []*Client

			s.mu.RLock()

			for _, client := range s.clients {
				if client.room != msg.room {
					continue
				}
				select {
				case client.out <- msg.data:
				default:
					slow = append(slow, client)
				}
			}
			s.mu.RUnlock()

			for _, c := range slow {
				c.kick("too slow")
			}
		}
	}
}

// announce broadcasts a presence change for c to whatever room c is in now.
func (s *Server) announce(c *Client, what string) {
	msg := Message{Username: c.username, Text: what, Room: c.room, CreatedAt: time.Now()}
	s.broadcast <- BroadcastMsg{room: c.room, data: ToByteArray(msg)}
}

// replayHistory queues the tail of c's current room to c alone.
func (s *Server) replayHistory(c *Client) {
	history, err := s.store.GetMessages(c.room, historyLimit)
	if err != nil {
		log.Printf("error loading history for #%s: %v", c.room, err)
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
	// through out. That gives conn exactly one writer, which is the invariant
	// 2.1 exists to establish.
	client := &Client{
		conn: conn,
		room: "general",
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
		// Graceful, not kick: the client must actually receive the reason. A
		// bare conn.Close() here would discard the queued message.
		client.closeGracefully()
		return
	}
	client.username = username

	// --- registration: from here the client is reachable by broadcasts ---

	s.mu.Lock()
	s.clients[conn] = client
	s.mu.Unlock()

	defer func() {
		// Deregister before closing out. delete takes the write lock, which
		// cannot be acquired while broadcastLoop holds the read lock, so by the
		// time this returns no broadcast can still hold a reference to this
		// client — which is what stops close(c.out) racing with a send on it.
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()

		s.announce(client, "has left")
		client.closeGracefully()
	}()

	s.replayHistory(client)
	s.announce(client, "has joined")
	client.queuef("You are in #%s. Use /join <room> to switch rooms.\n", client.room)

	// --- message loop ---

	for scanner.Scan() {
		text := scanner.Text()

		if strings.HasPrefix(text, "/join") {
			newRoom := strings.TrimSpace(strings.TrimPrefix(text, "/join "))
			if newRoom == "" {
				client.queuef("Usage: /join <room>\n")
				continue
			}

			s.announce(client, "has left")

			s.mu.Lock()
			client.room = newRoom
			s.mu.Unlock()

			s.replayHistory(client)
			s.announce(client, "has joined")
			client.queuef("Switched to #%s\n", newRoom)
			continue
		}

		// Regular message — persist and broadcast.
		msg := Message{Username: username, Text: text, Room: client.room, CreatedAt: time.Now()}
		if _, err := s.store.SaveMessage(client.room, username, text); err != nil {
			log.Printf("error saving message: %v", err)
		}
		s.broadcast <- BroadcastMsg{room: client.room, data: ToByteArray(msg)}
	}
}
