package chat

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func NewStore(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return s, nil
}

func (s *Store) migrate() error {
	schema := `
    CREATE TABLE IF NOT EXISTS messages (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      room       TEXT NOT NULL DEFAULT 'general',
      username   TEXT NOT NULL,
      content    TEXT NOT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_messages_room_ts ON messages(room, created_at);
    `
	_, err := s.db.Exec(schema)
	return err
}

func (s *Store) SaveMessage(room, username, content string) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO messages (room, username, content, created_at) VALUES (?, ?, ?,
	?)`,
		room, username, content, time.Now().UTC(),
	)
	if err != nil {
		return 0, fmt.Errorf("insert message: %w", err)
	}
	return res.LastInsertId()
}

func (s *Store) GetMessages(room string, limit int) ([]Message, error) {
	rows, err := s.db.Query(`
      SELECT id, room, username, content, created_at
      FROM messages
      WHERE room = ?
      ORDER BY created_at DESC
      LIMIT ?
    `, room, limit)
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Room, &m.Username, &m.Text, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		messages = append(messages, m)
	}

	// Reverse to oldest-first for display.
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}

	return messages, rows.Err()
}

func (s *Store) Close() error {
	return s.db.Close()
}
