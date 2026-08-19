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

type Client struct {
	conn     net.Conn
	username string
	room     string
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
			s.mu.RLock()
			for _, client := range s.clients {
				if client.room == msg.room {
					client.conn.Write(msg.data)
				}
			}
			s.mu.RUnlock()
		}
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	// Get username.
	fmt.Fprint(conn, "Enter username: \n")
	if !scanner.Scan() {
		return
	}
	username := strings.TrimSpace(scanner.Text())
	if username == "" {
		fmt.Fprintln(conn, "username cannot be empty")
		return
	}

	room := "general"

	client := &Client{conn: conn, username: username, room: room}

	s.mu.Lock()
	s.clients[conn] = client
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()
		msg := Message{Username: username, Text: "has left", Room: room, CreatedAt: time.Now()}
		s.broadcast <- BroadcastMsg{room: room, data: ToByteArray(msg)}
	}()

	// Send chat history to new client.
	history, err := s.store.GetMessages(room, 50)
	if err != nil {
		log.Printf("error loading history: %v", err)
	} else {
		for _, m := range history {
			conn.Write(ToByteArray(m))
		}
	}

	// Announce join.
	joinMsg := Message{Username: username, Text: "has joined", Room: room,
		CreatedAt: time.Now()}
	s.broadcast <- BroadcastMsg{room: room, data: ToByteArray(joinMsg)}

	fmt.Fprintf(conn, "You are in #%s. Use /join <room> to switch rooms.\n", room)

	// Message loop.
	for scanner.Scan() {
		text := scanner.Text()

		// Handle /join command.
		if strings.HasPrefix(text, "/join") {
			newRoom := strings.TrimSpace(strings.TrimPrefix(text, "/join "))
			if newRoom == "" {
				fmt.Fprintln(conn, "Usage: /join <room>")
				continue
			}

			// Announce leaving old room.
			leaveMsg := Message{Username: username, Text: "has left", Room: client.room, CreatedAt: time.Now()}
			s.broadcast <- BroadcastMsg{room: client.room, data: ToByteArray(leaveMsg)}

			// Switch room.
			s.mu.Lock()
			client.room = newRoom
			s.mu.Unlock()
			room = newRoom

			// Send new room history.
			history, err := s.store.GetMessages(room, 50)
			if err != nil {
				log.Printf("error loading history: %v", err)
			} else {
				for _, m := range history {
					conn.Write(ToByteArray(m))
				}
			}

			// Announce joining new room.
			joinMsg := Message{Username: username, Text: "has joined", Room: room,
				CreatedAt: time.Now()}
			s.broadcast <- BroadcastMsg{room: room, data: ToByteArray(joinMsg)}
			fmt.Fprintf(conn, "Switched to #%s\n", room)
			continue
		} else {
			// Regular message — persist and broadcast.
			msg := Message{Username: username, Text: text, Room: room, CreatedAt: time.Now()}
			if _, err := s.store.SaveMessage(room, username, text); err != nil {
				log.Printf("error saving message: %v", err)
			}
			s.broadcast <- BroadcastMsg{room: room, data: ToByteArray(msg)}
		}
	}
}
