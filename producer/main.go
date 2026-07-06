package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// services and their plausible log messages per severity.
var services = []string{"auth", "payment", "orders", "inventory"}

var severities = []string{"info", "warning", "error", "critical"}

var sampleMessages = map[string][]string{
	"info":     {"request processed successfully", "cache warmed", "health check passed"},
	"warning":  {"latency above threshold", "retrying downstream call", "cache miss rate high"},
	"error":    {"failed to process request", "downstream timeout", "validation failed"},
	"critical": {"service unresponsive", "database connection lost", "out of memory"},
}

// severityWeights biases random picks so most logs are info/warning,
var severityWeights = []int{60, 25, 10, 5}

func randomSeverity() string {
	total := 0
	for _, w := range severityWeights {
		total += w
	}
	r := rand.Intn(total)
	for i, w := range severityWeights {
		if r < w {
			return severities[i]
		}
		r -= w
	}
	return severities[0]
}

func main() {
	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
		amqpURL = "amqp://logs_user:logs_pass@localhost:5672/"
	}

	publisher, err := NewPublisher(amqpURL)
	if err != nil {
		log.Fatalf("failed to create publisher: %v", err)
	}
	defer publisher.Close()

	log.Println("producer connected, publishing mock logs... (Ctrl+C to stop)")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down producer")
			return
		case <-ticker.C:
			service := services[rand.Intn(len(services))]
			severity := randomSeverity()
			messages := sampleMessages[severity]
			message := messages[rand.Intn(len(messages))]

			entry := LogEntry{
				Service:   service,
				Severity:  severity,
				Message:   message,
				Timestamp: time.Now(),
				Metadata: map[string]interface{}{
					"host": "mock-host-01",
				},
			}

			if err := publisher.Publish(ctx, entry); err != nil {
				log.Printf("publish error: %v", err)
				continue
			}
			log.Printf("published [%s] %s: %s", entry.RoutingKey(), entry.Service, entry.Message)
		}
	}
}
