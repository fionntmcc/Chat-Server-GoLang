package chat_server

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

func TestClientConnectAndJoin(t *testing.T) {
	server, err := NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.Start()
	defer server.Shutdown()

	addr := server.listener.Addr().String()

	conn, reader := connectClient(t, addr, "alice")
	defer conn.Close()

	// Read lines until we see the join announcement.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	found := false
	for i := 0; i < 10; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, "alice") && strings.Contains(line,
			"has joined") {
			found = true
			break
		}
	}

	if !found {
		t.Error("expected join announcement for alice")
	}
}

func TestBroadcastBetweenClients(t *testing.T) {
	server, err := NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.Start()
	defer server.Shutdown()

	addr := server.listener.Addr().String()

	// Connect two clients.
	conn1, reader1 := connectClient(t, addr, "alice")
	defer conn1.Close()

	conn2, reader2 := connectClient(t, addr, "bob")
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

	// Alice sends a message.
	fmt.Fprintln(conn1, "hello from alice")

	// Bob should receive it.
	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader2.ReadString('\n')
	if err != nil {
		t.Fatalf("bob failed to receive message: %v", err)
	}

	if !strings.Contains(line, "hello from alice") {
		t.Errorf("expected bob to receive alice's message, got: %s",
			line)
	}

	if !strings.Contains(line, "alice") {
		t.Errorf("expected message to contain username, got: %s",
			line)
	}
}
