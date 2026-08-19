package chat

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// helper to connect a client and set username
func connectClient(t *testing.T, addr, username string) (net.Conn,
	*bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	reader := bufio.NewReader(conn)

	// Read the "Enter username:" prompt.
	_, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read prompt: %v", err)
	}

	// Send username.
	fmt.Fprintln(conn, username)

	return conn, reader
}

// drainLines reads and discards all pending lines using a short timeout.
func drainLines(reader *bufio.Reader, conn net.Conn) {
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	for {
		_, err := reader.ReadString('\n')
		if err != nil {
			break
		}
	}
}

func TestClientConnectAndJoin(t *testing.T) {
	server, err := NewServer("127.0.0.1:0", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	go server.Start()
	defer server.Shutdown()

	addr := server.listener.Addr().String()

	conn, reader := connectClient(t, addr, "fionn")
	defer conn.Close()

	// Read lines until we see the join announcement.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	found := false
	for i := 0; i < 10; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, "fionn") && strings.Contains(line,
			"has joined") {
			found = true
			break
		}
	}

	if !found {
		t.Error("expected join announcement for fionn")
	}
}

func TestBroadcastBetweenClients(t *testing.T) {
	server, err := NewServer("127.0.0.1:0", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	go server.Start()
	defer server.Shutdown()

	addr := server.listener.Addr().String()

	// Connect two clients.
	conn1, reader1 := connectClient(t, addr, "fionn")
	defer conn1.Close()

	conn2, reader2 := connectClient(t, addr, "liv")
	defer conn2.Close()

	// Drain any join messages on both readers.
	conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < 10; i++ {
		reader1.ReadString('\n')
		if reader1.Buffered() == 0 {
			break
		}
	}
	for i := 0; i < 10; i++ {
		reader2.ReadString('\n')
		if reader2.Buffered() == 0 {
			break
		}
	}

	// fionn sends a message.
	fmt.Fprintln(conn1, "hello from fionn")

	// liv should receive it.
	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader2.ReadString('\n')
	if err != nil {
		t.Fatalf("liv failed to receive message: %v", err)
	}

	if !strings.Contains(line, "hello from fionn") {
		t.Errorf("expected liv to receive fionn's message, got: %s",
			line)
	}

	if !strings.Contains(line, "fionn") {
		t.Errorf("expected message to contain username, got: %s",
			line)
	}
}

func TestCreateNewRoom(t *testing.T) {
	server, err := NewServer("127.0.0.1:0", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	go server.Start()
	defer server.Shutdown()

	addr := server.listener.Addr().String()

	// Connect two clients.
	conn1, reader1 := connectClient(t, addr, "fionn")
	defer conn1.Close()

	conn2, reader2 := connectClient(t, addr, "liv")
	defer conn2.Close()

	drainLines(reader1, conn1)
	drainLines(reader2, conn2)

	// fionn joins a new room.
	fmt.Fprintln(conn1, "/join testing")

	// Both users should be notified of new room.
	conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	line1, err := reader1.ReadString('\n')
	if err != nil {
		t.Fatalf("fionn failed to receive message: %v", err)
	}

	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	line2, err := reader2.ReadString('\n')
	if err != nil {
		t.Fatalf("liv failed to receive message: %v", err)
	}

	if !strings.Contains(line1, "Switched to #testing") {
		t.Errorf("expected fionn to receive \"Switched to testing\" message, got: %s", line1)
	}
	if !strings.Contains(line2, "fionn: has left") {
		t.Errorf("expected olivia to receive has left message, got: %s", line2)
	}
}
