# chat-server

A TCP-based multi-room chat server written in Go. 
It has SQLite-backed message persistence.

## Features

- Multi-room support with `/join <room>` command
- Message history replayed on join
- Persistent storage via SQLite (Write-Ahead Logging mode)
- Concurrent client handling

## Requirements

- Go 1.25+

## Build and run

#### Clone repo:
```bash
  git clone https://github.com/fionntmcc/Chat-Server-GoLang.git
```

#### Enter project:
```bash
  cd Chat-Server-GoLang
```

#### Run server
```bash
  go run ./cmd/chat-server <server-address> <db-name.db>
```

#### Run client
```bash
go build ./cmd/chat-client <server-address:port>
```
