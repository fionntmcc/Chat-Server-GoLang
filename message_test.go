package chat_server

import (
	"testing"
	"time"
)

func TestToByteArray(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
		want string
	}{
		{
			name: "normal message",
			msg:  Message{Username: "alice", Text: "hello", CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)},
			want: "[12:00:00] alice: hello\n",
		},
		{
			name: "empty text",
			msg:  Message{Username: "bob", Text: "", CreatedAt: time.Date(2026, 1, 1, 9, 30, 0, 0, time.UTC)},
			want: "[09:30:00] bob: \n",
		},
		{
			name: "special characters",
			msg: Message{Username: "user", Text: "héllo 🌍",
				CreatedAt: time.Date(2026, 6, 15, 23, 59, 59, 0, time.UTC)},
			want: "[23:59:59] user: héllo 🌍\n",
		},
		{
			name: "username with spaces",
			msg:  Message{Username: "john doe", Text: "hi", CreatedAt: time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC)},
			want: "[00:00:00] john doe: hi\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(ToByteArray(tt.msg))
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
