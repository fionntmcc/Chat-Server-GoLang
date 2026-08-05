package chat_server

import "time"

type Message struct {
	sender string
	text   string
	time   time.Time
}
