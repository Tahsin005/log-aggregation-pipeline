package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchangeName = "logs_topic_exchange"
	exchangeType = "topic"
	queueName    = "all_logs_storage_queue"
	bindingKey   = "#" // catch every service + severity combination
)

func main() {
	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
		amqpURL = "amqp://logs_user:logs_pass@localhost:5672/"
	}

	pgDSN := os.Getenv("POSTGRES_DSN")
	if pgDSN == "" {
		pgDSN = "postgres://logs_user:logs_pass@localhost:5433/logs_db"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	writer, err := NewStorageWriter(ctx, pgDSN)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer writer.Close()

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatalf("failed to dial rabbitmq: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(exchangeName, exchangeType, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("failed to declare exchange: %v", err)
	}

	queue, err := ch.QueueDeclare(
		queueName,
		true,  // durable — survives broker restart
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatalf("failed to declare queue: %v", err)
	}

	err = ch.QueueBind(queue.Name, bindingKey, exchangeName, false, nil)
	if err != nil {
		log.Fatalf("failed to bind queue: %v", err)
	}

	err = ch.Qos(10, 0, false)
	if err != nil {
		log.Fatalf("failed to set QoS: %v", err)
	}

	deliveries, err := ch.Consume(
		queue.Name,
		"storage-writer", // consumer tag
		false,            // autoAck — false, we ack manually after successful insert
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		log.Fatalf("failed to register consumer: %v", err)
	}

	log.Println("storage_writer consuming from", queue.Name, "bound to", bindingKey)

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down storage_writer")
			return

		case delivery, ok := <-deliveries:
			if !ok {
				log.Println("delivery channel closed")
				return
			}

			var entry LogEntry
			if err := json.Unmarshal(delivery.Body, &entry); err != nil {
				log.Printf("failed to unmarshal message, discarding: %v", err)
				// malformed message
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