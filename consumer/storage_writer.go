package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type LogEntry struct {
	Service   string                 `json:"service"`
	Severity  string                 `json:"severity"`
	Message   string                 `json:"message"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

type StorageWriter struct {
	pool *pgxpool.Pool
}

// NewStorageWriter opens a connection pool to Postgres.
func NewStorageWriter(ctx context.Context, dsn string) (*StorageWriter, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &StorageWriter{pool: pool}, nil
}

func (w *StorageWriter) Insert(ctx context.Context, entry LogEntry, routingKey string) error {
	metadataJSON, err := json.Marshal(entry.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	const query = `
		INSERT INTO logs (service, severity, message, routing_key, metadata, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err = w.pool.Exec(ctx, query,
		entry.Service,
		entry.Severity,
		entry.Message,
		routingKey,
		metadataJSON,
		entry.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("insert log entry: %w", err)
	}

	return nil
}

func (w *StorageWriter) Close() {
	if w.pool != nil {
		w.pool.Close()
	}
}

func logInsertError(entry LogEntry, routingKey string, err error) {
	log.Printf("failed to store log [%s] %s: %v", routingKey, entry.Service, err)
}