package chat_server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type Server struct {
	listener  net.Listener
	clients   map[net.Conn]string // connection -> username
	mu        sync.RWMutex
	broadcast chan Message
	quit      chan bool
}

func NewServer(address string) (*Server, error) {
	listener, err := net.Listen("tcp", address)

	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %v", address, err)
	}

	return &Server{
		listener:  listener,
		clients:   make(map[net.Conn]string),
		broadcast: make(chan Message),
		quit:      make(chan bool),
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

	s.mu.RLock()
	for conn := range s.clients {
		conn.Close()
	}
}

func (s *Server) broadcastLoop() {
	for {
		select {
		case <-s.quit:
			return
		case msg := <-s.broadcast:
			s.mu.RLock()
			for conn := range s.clients {
				conn.Write(msg)
			}
			s.mu.RUnlock()
		}
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	fmt.Fprint(conn, "Enter username: ")
	if !scanner.Scan() {
		return
	}
	username := scanner.Text()

	s.mu.Lock()
	s.clients[conn] = username
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()
		s.broadcast <- Message{sender: username, text: "has left", sentAt: time.Now()}
	}()

	s.broadcast <- Message{sender: username, text: "has joined", sentAt: time.Now()}

	for scanner.Scan() {
		s.broadcast <- Message{sender: username, text: scanner.Text(), sentAt: time.Now()}
	}
}
