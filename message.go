package chat_server

import (
	"fmt"
	"time"
)

type Message struct {
	sender string
	text   string
	sentAt time.Time
}

func ToByteArray(msg Message) []byte {
	formatted := fmt.Sprintf("[%s] %s: %s\n", msg.sentAt.Format("15:04:05"), msg.sender, msg.text)
	return []byte(formatted)
}
