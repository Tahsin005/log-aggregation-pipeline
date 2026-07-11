package main

import (
	"context"
	"encoding/json"
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

const (
	retryExchangeName = "logs_retry_exchange"
	retryQueueName    = "all_logs_retry_queue"
	retryTTLMillis    = int32(5000) // 5s before a failed message is retried

	finalDLQExchangeName = "logs_final_dlq_exchange"
	finalDLQQueueName    = "all_logs_final_dlq"

	maxRetryAttempts = 3
)

type StorageWriter struct {
	pool *pgxpool.Pool
}

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

func setupTopology(ch *amqp.Channel) (mainQueue amqp.Queue, err error) {
	if err := ch.ExchangeDeclare(exchangeName, exchangeType, true, false, false, false, nil); err != nil {
		return amqp.Queue{}, fmt.Errorf("declare topic exchange: %w", err)
	}

	if err := ch.ExchangeDeclare(retryExchangeName, "fanout", true, false, false, false, nil); err != nil {
		return amqp.Queue{}, fmt.Errorf("declare retry exchange: %w", err)
	}

	retryQueue, err := ch.QueueDeclare(
		retryQueueName,
		true, false, false, false,
		amqp.Table{
			"x-message-ttl":            retryTTLMillis,
			"x-dead-letter-exchange":   exchangeName,
		},
	)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("declare retry queue: %w", err)
	}

	if err := ch.QueueBind(retryQueue.Name, "", retryExchangeName, false, nil); err != nil {
		return amqp.Queue{}, fmt.Errorf("bind retry queue: %w", err)
	}

	if err := ch.ExchangeDeclare(finalDLQExchangeName, "fanout", true, false, false, false, nil); err != nil {
		return amqp.Queue{}, fmt.Errorf("declare final dlq exchange: %w", err)
	}

	finalDLQ, err := ch.QueueDeclare(finalDLQQueueName, true, false, false, false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("declare final dlq queue: %w", err)
	}

	if err := ch.QueueBind(finalDLQ.Name, "", finalDLQExchangeName, false, nil); err != nil {
		return amqp.Queue{}, fmt.Errorf("bind final dlq queue: %w", err)
	}

	mainQueue, err = ch.QueueDeclare(
		queueName,
		true, false, false, false,
		amqp.Table{"x-dead-letter-exchange": retryExchangeName},
	)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("declare main queue: %w", err)
	}

	if err := ch.QueueBind(mainQueue.Name, bindingKey, exchangeName, false, nil); err != nil {
		return amqp.Queue{}, fmt.Errorf("bind main queue: %w", err)
	}

	return mainQueue, nil
}

func retryCount(delivery amqp.Delivery) int64 {
	xDeath, ok := delivery.Headers["x-death"].([]interface{})
	if !ok {
		return 0
	}

	for _, raw := range xDeath {
		entry, ok := raw.(amqp.Table)
		if !ok {
			continue
		}
		queue, _ := entry["queue"].(string)
		reason, _ := entry["reason"].(string)
		if queue == queueName && reason == "rejected" {
			if count, ok := entry["count"].(int64); ok {
				return count
			}
		}
	}
	return 0
}

func publishToFinalDLQ(ctx context.Context, ch *amqp.Channel, delivery amqp.Delivery) error {
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return ch.PublishWithContext(
		publishCtx,
		finalDLQExchangeName,
		"",
		false, false,
		amqp.Publishing{
			ContentType:  delivery.ContentType,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Headers:      delivery.Headers,
			Body:         delivery.Body,
		},
	)
}

func RunStorageWriter(ctx context.Context, conn *amqp.Connection, writer *StorageWriter) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	mainQueue, err := setupTopology(ch)
	if err != nil {
		return err
	}

	if err := ch.Qos(10, 0, false); err != nil {
		return fmt.Errorf("set qos: %w", err)
	}

	deliveries, err := ch.Consume(mainQueue.Name, "storage-writer", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("register consumer: %w", err)
	}

	log.Println("storage_writer consuming from", mainQueue.Name,
		"| retry queue:", retryQueueName, fmt.Sprintf("(TTL=%dms, max attempts=%d)", retryTTLMillis, maxRetryAttempts),
		"| final DLQ:", finalDLQQueueName)

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
				log.Printf("malformed message, sending straight to final DLQ: %v", err)
				if err := publishToFinalDLQ(ctx, ch, delivery); err != nil {
					log.Printf("failed to publish to final DLQ, requeuing as fallback: %v", err)
					delivery.Nack(false, true)
					continue
				}
				delivery.Ack(false)
				continue
			}

			if err := writer.Insert(ctx, entry, delivery.RoutingKey); err != nil {
				logInsertError(entry, delivery.RoutingKey, err)

				attempts := retryCount(delivery)
				if attempts >= maxRetryAttempts {
					log.Printf("exhausted %d retries, giving up on [%s], routing to final DLQ",
						maxRetryAttempts, delivery.RoutingKey)
					if err := publishToFinalDLQ(ctx, ch, delivery); err != nil {
						log.Printf("failed to publish to final DLQ, requeuing as fallback: %v", err)
						delivery.Nack(false, true)
						continue
					}
					delivery.Ack(false)
					continue
				}

				log.Printf("retry %d/%d for [%s], will retry in %dms",
					attempts+1, maxRetryAttempts, delivery.RoutingKey, retryTTLMillis)
				delivery.Nack(false, false) // -> retry exchange -> TTL queue -> back to main queue
				continue
			}

			delivery.Ack(false)
		}
	}
}