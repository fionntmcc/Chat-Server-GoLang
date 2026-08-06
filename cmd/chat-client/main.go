package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: server-address <addr>")
	}
	addr := os.Args[1]

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	println("Connected to chat server")

	// Goroutine to read messages from server and print to console
	go func() {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			log.Println(scanner.Text())
		}
		fmt.Println("Connection closed")
		os.Exit(0)
	}()

	// Read messages from console and send to server
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fmt.Fprintf(conn, "%s\n", scanner.Text())
	}
}
