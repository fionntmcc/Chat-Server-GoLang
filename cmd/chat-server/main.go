package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)
import chat "github.com/fionntmcc/chat-Server-GoLang"

func main() {
	addr := flag.String("addr", ":8080", "server listen address")
	flag.Parse()

	server, err := chat.NewServer(*addr)

	if err != nil {
		log.Fatal(err)
	}

	go server.Start()

	log.Println("chat server running on", *addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	server.Shutdown()
}
