package chat

import (
	"testing"
)

func TestSaveAndGetMessages(t *testing.T) {
	store, err := NewStore(":memory:")

	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.SaveMessage("general", "fionn", "Hi")
	store.SaveMessage("general", "liv", "Hi2")

	msgs, err := store.GetMessages("general", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("Expected 2 messages, got %d", len(msgs))
	}

	if msgs[0].Username != "fionn" {
		t.Fatalf("Expected message to be from fionn, got %s", msgs[0].Username)
	}
	if msgs[0].Text != "Hi" {
		t.Fatalf("Expected message to be 'Hi', got %s", msgs[0].Text)
	}
}

func TestLimitCutoff(t *testing.T) {
	store, err := NewStore(":memory:")

	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.SaveMessage("general", "fionn1", "Hi1")
	store.SaveMessage("general", "fionn2", "Hi2")
	store.SaveMessage("general", "fionn3", "Hi3")
	store.SaveMessage("general", "fionn4", "Hi4")
	store.SaveMessage("general", "fionn5", "Hi5")

	msgs, err := store.GetMessages("general", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("Expected 3 messages, got %d", len(msgs))
	}

	if msgs[0].Username != "fionn3" {
		t.Fatalf("Expected message to be from fionn3, got %s", msgs[0].Username)
	}
	if msgs[0].Text != "Hi3" {
		t.Fatalf("Expected message to be 'Hi3', got %s", msgs[0].Text)
	}
	if msgs[1].Username != "fionn4" {
		t.Fatalf("Expected message to be from fionn4, got %s", msgs[0].Username)
	}
	if msgs[1].Text != "Hi4" {
		t.Fatalf("Expected message to be 'Hi4', got %s", msgs[0].Text)
	}
}

func TestRoomMessages(t *testing.T) {
	store, err := NewStore(":memory:")

	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.SaveMessage("room1", "fionn1", "Hi1")
	store.SaveMessage("room2", "fionn2", "Hi2")
	store.SaveMessage("room1", "fionn3", "Hi3")
	store.SaveMessage("room2", "fionn4", "Hi4")
	store.SaveMessage("room1", "fionn5", "Hi5")

	// room 1
	msgsR1, err := store.GetMessages("room1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgsR1) != 3 {
		t.Fatalf("Expected 3 messages, got %d", len(msgsR1))
	}

	for _, msg := range msgsR1 {
		if msg.Room != "room1" {
			t.Fatalf("Expected all messages to be in room1, got %s", msg.Room)
		}
	}

	// room 2
	msgsR2, err := store.GetMessages("room2", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgsR2) != 2 {
		t.Fatalf("Expected 2 messages, got %d", len(msgsR2))
	}

	for _, msg := range msgsR2 {
		if msg.Room != "room2" {
			t.Fatalf("Expected all messages to be in room2, got %s", msg.Room)
		}
	}
}

func TestGetNonExistentRoom(t *testing.T) {
	store, err := NewStore(":memory:")

	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	msgs, err := store.GetMessages("room1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("Expected 0 messages, got %d", len(msgs))
	}
}

func TestNewStore_InvalidPath(t *testing.T) {
	// Path that can't be created — triggers open/ping failure.
	store, err := NewStore("/nonexistent/dir/chat.db")
	if err == nil {
		store.Close()
		t.Fatal("expected error for invalid path")
	}
}

func TestSaveMessage_ClosedDB(t *testing.T) {
	store, err := NewStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	_, err = store.SaveMessage("general", "fionn", "hi")
	if err == nil {
		t.Fatal("expected error on closed db")
	}
}

func TestGetMessages_ClosedDB(t *testing.T) {
	store, err := NewStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	_, err = store.GetMessages("general", 10)
	if err == nil {
		t.Fatal("expected error on closed db")
	}
}
