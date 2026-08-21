package chat

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// Default timeout for expectLine. Long enough to absorb CI jitter, short enough
// that a genuine failure does not stall the suite.
const defaultTimeout = 2 * time.Second

// testClient is a chat client for use in tests. A background goroutine reads
// from the connection and pumps each line into c.lines, which decouples message
// arrival from assertion: lines queue up whether or not a test is currently
// waiting on one. This is what lets expectLine read *until* a match instead of
// asserting on whatever line happens to arrive first.
//
// Invariant: nothing except the pump goroutine reads from conn.
type testClient struct {
	t    *testing.T
	conn net.Conn

	// lines receives one entry per line received from the server, with the
	// trailing newline stripped. Closed when the connection ends (EOF or error),
	// so a receive on a closed channel signals disconnect rather than blocking.
	lines chan string

	// done is closed by cleanup to release a pump goroutine that is blocked
	// sending into a full lines channel. Without this, conn.Close() alone would
	// not unblock the pump and the goroutine would leak — which goleak will
	// catch in Phase 3.
	done chan struct{}
}

// dial opens a connection to addr and starts the reader pump. Teardown is
// registered with t.Cleanup, so callers never write `defer conn.Close()`.
//
// Cleanup ordering matters: t.Cleanup functions run LIFO, so the client
// registered here is torn down before the server registered by newTestServer,
// which is the order you want (clients disconnect, then the server shuts down).
func dial(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	c := &testClient{
		t:     t,
		conn:  conn,
		lines: make(chan string, 256),
		done:  make(chan struct{}),
	}

	go c.pump()

	t.Cleanup(func() {
		close(c.done)
		c.conn.Close()
	})

	return c
}

// dialRaw opens a connection and deliberately does NOT start a reader pump, so
// the peer's writes accumulate in the socket buffers until they block. This is
// the "slow client" used by the head-of-line-blocking test in subtask 1.5;
// testClient can never serve that role because it always drains.
func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}

	t.Cleanup(func() { conn.Close() })

	return conn
}

// pump reads lines from the connection until EOF or error and forwards them to
// c.lines. It owns all reads on c.conn.
func (c *testClient) pump() {
	defer close(c.lines)

	scanner := bufio.NewScanner(c.conn)

	for scanner.Scan() {
		select {
		case c.lines <- scanner.Text():
		case <-c.done:
			return
		}
	}
}

// login completes the username handshake: wait for the server's prompt, send the
// name, then wait for the room banner.
//
// Waiting for the banner is not cosmetic. handleConn inserts the client into
// s.clients *before* writing the banner, so receiving it proves registration has
// completed. Without that wait, login returns as soon as the name hits the
// socket, and a test that immediately starts broadcasting will have messages
// delivered to a room this client has not joined yet — losing exactly the
// earliest messages. The protocol has no explicit "ready" frame; the banner is
// standing in for one until Phase 7.1 defines the handshake properly.
func (c *testClient) login(name string) {
	c.t.Helper()

	c.expectLine(defaultTimeout, contains("username"))
	c.send(name)
	c.expectLine(defaultTimeout, contains("You are in #"))
}

// send writes a single line to the server.
func (c *testClient) send(line string) {
	c.t.Helper()

	if _, err := fmt.Fprintln(c.conn, line); err != nil {
		c.t.Fatalf("send %q: %v", line, err)
	}
}

// expectLine consumes lines until one satisfies match, and returns it.
//
// Indexing [0] is safe because expectCount only returns nil after calling
// t.Fatalf, which calls runtime.Goexit and never returns. If that Fatalf is ever
// downgraded to Errorf, this becomes an index-out-of-range panic.
func (c *testClient) expectLine(timeout time.Duration, match func(string) bool) string {
	c.t.Helper()
	return c.expectCount(1, timeout, match)[0]
}

// expectNoLine asserts that no line satisfying match arrives within timeout.
// Used for negative assertions that the current test helpers cannot express at
// all: a client in one room must not receive another room's traffic, and a
// kicked client must receive nothing further.
//
// Note this always costs the full timeout, so keep it short (100-200ms) and use
// it sparingly.
func (c *testClient) expectNoLine(timeout time.Duration, match func(string) bool) {
	c.t.Helper()

	deadline := time.After(timeout)
	var seen []string
	for {
		select {
		case line, ok := <-c.lines:
			if !ok {
				return
			}
			if match(line) {
				c.t.Fatalf("unexpected line %q (after %d others: %q)", line, len(seen), seen)
				return
			}
			seen = append(seen, line)
		case <-deadline:
			return
		}
	}
}

// --- predicate builders -----------------------------------------------------
//
// Small constructors keep assertions readable at the call site:
//
//	c.expectLine(defaultTimeout, both(contains("fionn"), contains("has joined")))

// contains matches lines containing sub.
func contains(sub string) func(string) bool {
	return func(line string) bool { return strings.Contains(line, sub) }
}

// both matches lines satisfying every supplied predicate. Variadic so it also
// covers the three-condition cases (user + room + text).
func both(preds ...func(string) bool) func(string) bool {
	return func(line string) bool {
		for _, p := range preds {
			if !p(line) {
				return false
			}
		}
		return true
	}
}

// chatLine matches a rendered message from user containing text, i.e. it
// encodes knowledge of ToByteArray's "[15:04:05] user: text" format in exactly
// one place. When Phase 7.3 changes the wire format, this is the only predicate
// that needs to change.
func chatLine(user, text string) func(string) bool {
	return both(contains(user+": "), contains(text))
}

// --- subtask 1.2 ------------------------------------------------------------
//
// newTestServer(t) belongs in this file too: start a server on 127.0.0.1:0 with
// an in-memory store, register Shutdown via t.Cleanup, and return it alongside
// its address. That step also adds an exported Addr() method to Server so tests
// stop reaching into the unexported s.listener field.

func newTestServer(t *testing.T) *Server {
	t.Helper()

	s, err := NewServer("127.0.0.1:0", ":memory:")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	go s.Start()
	t.Cleanup(s.Shutdown)

	return s
}

func Addr(s *Server) string {
	return s.listener.Addr().String()
}

// collect consumes lines until n of them satisfy match, returning those n in
// arrival order. On timeout or disconnect it returns what it found plus an error
// describing where it stopped.
//
// It never touches c.t, which is what makes it safe to call from a spawned
// goroutine. t.Fatalf and t.FailNow must run on the test's own goroutine; called
// elsewhere they trigger runtime.Goexit in the wrong place and Go reports
// "test executed panic(nil) or runtime.Goexit" instead of a useful failure.
//
// One deadline covers the whole call. Calling expectLine n times in a loop would
// instead permit n * timeout in the worst case — over half an hour for the 1000
// messages per client that the fan-out test in 1.4 expects.
func (c *testClient) collect(n int, timeout time.Duration, match func(string) bool) ([]string, error) {
	deadline := time.After(timeout)
	matched := make([]string, 0, n)

	skipped := 0
	var recent []string // tail of non-matching lines, for the error message

	for len(matched) < n {
		select {
		case line, ok := <-c.lines:
			if !ok {
				return matched, fmt.Errorf("connection closed after %d of %d matches; last non-matching: %q",
					len(matched), n, recent)
			}
			if match(line) {
				matched = append(matched, line)
				continue
			}
			// Keep only the tail: a 1000-line dump is unreadable, and the most
			// recent lines are the informative ones.
			skipped++
			recent = append(recent, line)
			if len(recent) > 5 {
				recent = recent[1:]
			}
		case <-deadline:
			return matched, fmt.Errorf("timeout after %s with %d of %d matches (%d non-matching, last: %q)",
				timeout, len(matched), n, skipped, recent)
		}
	}

	return matched, nil
}

// expectCount is collect with failure reporting, for use on the test goroutine.
//
// Semantics are "at least n": it returns as soon as the nth match arrives and
// says nothing about an (n+1)th. To assert exactly n, follow it with
// expectNoLine over a short window.
func (c *testClient) expectCount(n int, timeout time.Duration, match func(string) bool) []string {
	c.t.Helper()

	if n <= 0 {
		c.t.Fatalf("expectCount: n must be positive, got %d", n)
		return nil
	}

	matched, err := c.collect(n, timeout, match)
	if err != nil {
		c.t.Fatal(err)
		return nil
	}
	return matched
}
