package chat

import (
	"fmt"
	"testing"
)

func TestClientConnectAndJoin(t *testing.T) {
	server := newTestServer(t)
	addr := Addr(server)

	conn := dial(t, addr)
	conn.login("fionn")

	conn.expectLine(defaultTimeout, contains("has joined"))
}

func TestBroadcastBetweenClients(t *testing.T) {
	server := newTestServer(t)
	addr := Addr(server)

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
	addr := Addr(server)

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
	conn2.expectNoLine(defaultTimeout, contains("fionn: message1"))

	// liv joins different room
	conn2.send("/join different")
	conn2.send("message2")

	// liv sees joins message, fionn does not see a message
	conn2.expectLine(defaultTimeout, contains("liv: has joined"))
	conn1.expectNoLine(defaultTimeout, contains("liv: message2"))

	// liv joins different room
	conn2.send("/join testing")
	conn2.send("message3")

	// fionn sees joins message, fionn sees liv's message
	conn1.expectLine(defaultTimeout, contains("liv: has joined"))
	conn1.expectLine(defaultTimeout, contains("liv: message3"))
}

func TestMessagesUnderLoad(t *testing.T) {
	server := newTestServer(t)
	addr := Addr(server)

	// declare array of conns
	conns := []*testClient{}

	// connect and name
	for i := 0; i < 50; i++ {
		c := dial(t, addr)
		c.login(fmt.Sprintf("name%d", i))
		conns = append(conns, c)
	}

	// send 20 messages each
	for _, c := range conns {
		for i := 0; i < 20; i++ {
			c.send(fmt.Sprintf("message%d", i))
		}
	}

	// check that all connections received 1000 messages containing "message"
	for _, c := range conns {
		c.expectCount(1000, defaultTimeout, contains("message"))
	}
}
