package chat_server

import (
	"fmt"
	"time"
)

type Message struct {
	ID        int64
	Room      string
	Username  string
	Text      string
	CreatedAt time.Time
}

func ToByteArray(msg Message) []byte {
	formatted := fmt.Sprintf("[%s] %s: %s\n", msg.CreatedAt.Format("15:04:05"), msg.Username, msg.Text)
	return []byte(formatted)
}
