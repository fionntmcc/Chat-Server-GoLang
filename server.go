package chat_server

import (
	"net"
	"sync"
)

type Server struct {
	listener  net.Listener
	clients   map[net.Conn]string // connection -> username
	mu        sync.RWMutex
	broadcast chan []byte
}
