package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
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

	alerter, err := NewAlerter(conn)
	if err != nil {
		log.Fatalf("failed to set up alerter: %v", err)
	}
	defer alerter.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := RunStorageWriter(ctx, conn, writer); err != nil {
			log.Printf("storage_writer stopped with error: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := alerter.Run(ctx); err != nil {
			log.Printf("alerter stopped with error: %v", err)
		}
	}()

	log.Println("consumer running: storage_writer + alerter (Ctrl+C to stop)")
	wg.Wait()
	log.Println("consumer shut down cleanly")
}