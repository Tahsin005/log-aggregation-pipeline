package main

import (
	"encoding/json"
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

func RunStorageWriter(ctx context.Context, conn *amqp.Connection, writer *StorageWriter) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(exchangeName, exchangeType, true, false, false, false, nil); err != nil {
		return err
	}

	queue, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return err
	}

	if err := ch.QueueBind(queue.Name, bindingKey, exchangeName, false, nil); err != nil {
		return err
	}

	if err := ch.Qos(10, 0, false); err != nil {
		return err
	}

	deliveries, err := ch.Consume(queue.Name, "storage-writer", false, false, false, false, nil)
	if err != nil {
		return err
	}

	log.Println("storage_writer consuming from", queue.Name, "bound to", bindingKey)

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down storage_writer")
			return nil

		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}

			var entry LogEntry
			if err := json.Unmarshal(delivery.Body, &entry); err != nil {
				log.Printf("failed to unmarshal message, discarding: %v", err)
				delivery.Nack(false, false)
				continue
			}

			if err := writer.Insert(ctx, entry, delivery.RoutingKey); err != nil {
				logInsertError(entry, delivery.RoutingKey, err)
				delivery.Nack(false, true)
				continue
			}

			delivery.Ack(false)
		}
	}
}