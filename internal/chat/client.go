package chat

import (
	"net"
	"sync"
	"fmt"
	"time"
	"log"
)

type Client struct {
	conn      net.Conn
	username  string
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

func joinRoom(rooms map[string]map[*Client]struct{}, room string, c *Client) {
	members, ok := rooms[room]
	if !ok {
		// Rooms are created on first join, which is how /join <newroom>
		// implicitly creates one.
		members = make(map[*Client]struct{})
		rooms[room] = members
	}
	members[c] = struct{}{}
}

func leaveRoom(rooms map[string]map[*Client]struct{}, room string, c *Client) {
	members, ok := rooms[room]
	if !ok {
		return
	}
	delete(members, c)
	if len(members) == 0 {
		// Without this, rooms keeps one permanent entry per name anyone ever
		// typed and a /join typo becomes a leak.
		delete(rooms, room)
	}
}

// deliver hands data to every member. No lock is held, so kick is safe inline —
// the collect-then-kick dance in the old broadcastLoop existed only because an
// RWMutex read lock cannot be upgraded to a write lock.
func deliver(members map[*Client]struct{}, data []byte) {
	for c := range members {
		select {
		case c.out <- data:
		default:
			c.kick("outbound buffer full")
		}
	}
}