package chat

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

func TestClientConnectAndJoin(t *testing.T) {
	server := newTestServer(t)
	addr := server.Addr()

	conn := dial(t, addr)
	conn.login("fionn")

	conn.expectLine(defaultTimeout, contains("has joined"))
}

func TestBroadcastBetweenClients(t *testing.T) {
	server := newTestServer(t)
	addr := server.Addr()

	conn1 := dial(t, addr)
	conn1.login("fionn")

	conn2 := dial(t, addr)
	conn2.login("liv")

	// fionn sends a message.
	conn1.send("hello from fionn")

	// liv should receive it.
	conn2.expectLine(defaultTimeout, contains("hello from fionn"))

	// liv sends a message.
	conn2.send("hi")

	// fionn should receive it.
	conn1.expectLine(defaultTimeout, contains("hi"))

}

func TestCreateNewRoom(t *testing.T) {
	server := newTestServer(t)
	addr := server.Addr()

	conn1 := dial(t, addr)
	conn1.login("fionn")

	conn2 := dial(t, addr)
	conn2.login("liv")

	// fionn joins room testing
	conn1.send("/join testing")
	conn1.send("message1")

	// liv sees left message, fionn sees joined message
	conn2.expectLine(defaultTimeout, contains("fionn: has left"))
	conn1.expectLine(defaultTimeout, contains("fionn: has joined"))
	conn2.expectNoLine(noLineWindow, contains("fionn: message1"))

	// liv joins different room
	conn2.send("/join different")
	conn2.send("message2")

	// liv sees joins message, fionn does not see a message
	conn2.expectLine(defaultTimeout, contains("liv: has joined"))
	conn1.expectNoLine(noLineWindow, contains("liv: message2"))

	// liv joins different room
	conn2.send("/join testing")
	conn2.send("message3")

	// fionn sees joins message, fionn sees liv's message
	conn1.expectLine(defaultTimeout, contains("liv: has joined"))
	conn1.expectLine(defaultTimeout, contains("liv: message3"))
}

// TestSlowClientDoesNotBlockOthers is the red test for defect 1.
//
// broadcastLoop writes directly to every client's conn while holding
// s.mu.RLock(). A client that never reads eventually fills its receive window
// and the server's send buffer, at which point that Write blocks — and because
// it blocks inside the broadcast loop, every other client in every room stops
// receiving. Nothing recovers until the slow client disconnects.
//
// This test is EXPECTED TO FAIL until Phase 2.1 gives each client its own writer
// goroutine and a bounded outbound queue with an explicit drop policy.
func TestSlowClientDoesNotBlockOthers(t *testing.T) {
	const (
		// The volume must exceed everything the kernel will buffer on slow's
		// behalf: the server's SO_SNDBUF (which autotunes into the megabytes) plus
		// slow's receive window. 2 MB was not enough and the test passed
		// spuriously. Large messages rather than many messages keeps the number of
		// SQLite inserts down. messageSize stays under bufio.Scanner's default
		// 64 KiB token limit, which the server has not raised yet (defect 4).
		fillerMessages = 400
		messageSize    = 40000
	)

	server := newTestServer(t)
	addr := server.Addr()

	// filler produces the traffic. It is a normal client so that its own copy of
	// every message can be discarded, keeping it from becoming a second slow
	// client and muddying attribution.
	filler := dial(t, addr)
	filler.login("filler")

	// slow logs in and then never reads a single byte. Shrinking SO_RCVBUF pins
	// the receive window small and disables window autotuning, so the server's
	// Write blocks after tens of kilobytes instead of megabytes.
	slow := dialRaw(t, addr)
	if tcp, ok := slow.(*net.TCPConn); ok {
		if err := tcp.SetReadBuffer(1024); err != nil {
			t.Fatalf("SetReadBuffer: %v", err)
		}
	}
	if _, err := fmt.Fprintln(slow, "slowpoke"); err != nil {
		t.Fatalf("slow login: %v", err)
	}

	// Wait for slow's join announcement to reach filler. slow never reads, so we
	// cannot wait on its banner; observing the broadcast is the only proof that
	// the server has registered it and will start writing to it.
	filler.expectLine(defaultTimeout, contains("slowpoke"))

	// From here filler's inbound traffic is discarded. The range ends when pump
	// closes the channel during cleanup, so no goroutine is leaked.
	go func() {
		for range filler.lines {
		}
	}()

	// Blast from a goroutine using raw writes, deliberately not filler.send:
	//   - once broadcastLoop is stuck, handleConn blocks pushing onto s.broadcast
	//     and stops reading filler's socket, so these writes will block too.
	//     Blocking here must not stall the test.
	//   - send() calls t.Fatalf, which is illegal off the test goroutine.
	// Cleanup closes the conn, which unblocks this and lets it exit.
	payload := strings.Repeat("x", messageSize)
	go func() {
		for i := 0; i < fillerMessages; i++ {
			if _, err := fmt.Fprintln(filler.conn, payload); err != nil {
				return
			}
		}
	}()

	// Let the jam establish. This is a genuine wait on OS socket buffering, not a
	// substitute for synchronisation: there is no signal the server can give us
	// that its Write has blocked.
	time.Sleep(250 * time.Millisecond)

	// observer arrives after the jam.
	//
	// Note where this actually fails today: not on the canary assertion below,
	// but on login. handleConn pushes the join announcement onto s.broadcast
	// (capacity 64) *before* writing the room banner, and that channel is full
	// because broadcastLoop is wedged. So the handshake itself stalls and the
	// banner never arrives. That is a stronger symptom than the one this test set
	// out to demonstrate: one client that will not read its socket stops new
	// clients from connecting at all.
	//
	// After Phase 2.1 both assertions should pass.
	observer := dial(t, addr)
	observer.login("observer")

	// observer's own message must come back to it via broadcastLoop. One small
	// write, so this cannot block.
	observer.send("canary")

	observer.expectLine(time.Second, contains("canary"))
}

// TestWhoAndRooms exercises the read side of the hub: handleConn cannot inspect
// room membership directly, so both commands go through request/reply.
func TestWhoAndRooms(t *testing.T) {
	server := newTestServer(t)
	addr := server.Addr()

	fionn := dial(t, addr)
	fionn.login("fionn")

	liv := dial(t, addr)
	liv.login("liv")

	fionn.send("/who")
	fionn.expectLine(defaultTimeout, both(contains("#general (2)"), contains("fionn, liv")))

	fionn.send("/rooms")
	fionn.expectLine(defaultTimeout, both(contains("rooms (1)"), contains("general")))

	// Waiting for liv's confirmation orders the switch ahead of the next query.
	liv.send("/join testing")
	liv.expectLine(defaultTimeout, contains("Switched to #testing"))

	fionn.send("/who")
	fionn.expectLine(defaultTimeout, both(contains("#general (1)"), contains("fionn")))

	fionn.send("/rooms")
	fionn.expectLine(defaultTimeout, both(contains("rooms (2)"), contains("general, testing")))

	// Prefix dispatch would have matched this against /who.
	fionn.send("/whoever")
	fionn.expectLine(defaultTimeout, chatLine("fionn", "/whoever"))
}

func TestMessagesUnderLoad(t *testing.T) {
	const (
		clients   = 50
		perClient = 20
	)
	total := clients * perClient

	server := newTestServer(t)
	addr := server.Addr()

	conns := make([]*testClient, 0, clients)
	for i := 0; i < clients; i++ {
		c := dial(t, addr)
		c.login(fmt.Sprintf("name%d", i))
		conns = append(conns, c)
	}

	// Consume concurrently with sending, not after it.
	//
	// Each client's outbound queue holds only outboundBuffer messages, so a
	// client that is not being drained while 1000 messages fan out to it is
	// correctly disconnected as too slow (2.1). Draining after all sends complete
	// would therefore be a test of the queue size, not of fan-out — and raising
	// outboundBuffer to make it pass would defeat the backpressure policy and
	// make TestSlowClientDoesNotBlockOthers vacuous.
	type result struct {
		name string
		err  error
	}
	results := make(chan result, clients)

	for i, c := range conns {
		go func() {
			// collect, not expectCount: t.Fatalf is illegal off the test goroutine.
			_, err := c.collect(total, 30*time.Second, contains("message"))
			results <- result{fmt.Sprintf("name%d", i), err}
		}()
	}

	for _, c := range conns {
		for i := 0; i < perClient; i++ {
			c.send(fmt.Sprintf("message%d", i))
		}
	}

	for range clients {
		if r := <-results; r.err != nil {
			t.Errorf("%s: %v", r.name, r.err)
		}
	}
}
