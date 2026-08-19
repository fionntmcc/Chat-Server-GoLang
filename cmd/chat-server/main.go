package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fionntmcc/chat-server/internal/chat"
)

func main() {
	addr := flag.String("addr", ":8080", "server listen address")
	dbPath := flag.String("db", "chat.db", "database file path")
	flag.Parse()

	server, err := chat.NewServer(*addr, *dbPath)

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
