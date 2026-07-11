package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchangeName      = "logs_topic_exchange"
	finalDLQQueueName = "all_logs_final_dlq"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "print what would be replayed without actually republishing or removing messages")
	flag.Parse()

	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
		amqpURL = "amqp://logs_user:logs_pass@localhost:5672/"
	}

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

	queueInfo, err := ch.QueueInspect(finalDLQQueueName)
	if err != nil {
		log.Fatalf("failed to inspect %s (does it exist yet?): %v", finalDLQQueueName, err)
	}

	if queueInfo.Messages == 0 {
		fmt.Println("final DLQ is empty, nothing to replay")
		return
	}

	fmt.Printf("found %d message(s) in %s\n", queueInfo.Messages, finalDLQQueueName)
	if *dryRun {
		fmt.Println("--dry-run set: inspecting only, nothing will be replayed or removed")
	}

	replayed, skipped := 0, 0

	for i := 0; i < queueInfo.Messages; i++ {
		delivery, ok, err := ch.Get(finalDLQQueueName, false)
		if err != nil {
			log.Fatalf("failed to get message: %v", err)
		}
		if !ok {
			break // queue drained early, nothing more to do
		}

		routingKey, _ := delivery.Headers["x-original-routing-key"].(string)
		if routingKey == "" {
			log.Printf("skipping message with no x-original-routing-key header (id=%v)", delivery.MessageId)
			delivery.Nack(false, true) // put it back, don't lose it
			skipped++
			continue
		}

		fmt.Printf("  [%d/%d] routing_key=%s body=%s\n", i+1, queueInfo.Messages, routingKey, truncate(string(delivery.Body), 100))

		if *dryRun {
			delivery.Nack(false, true) // dry run: leave the DLQ untouched
			continue
		}

		if err := republish(ch, routingKey, delivery); err != nil {
			log.Printf("failed to republish, leaving in DLQ: %v", err)
			delivery.Nack(false, true)
			skipped++
			continue
		}

		delivery.Ack(false) // remove from DLQ only after successful republish
		replayed++
	}

	fmt.Printf("\ndone: %d replayed, %d skipped\n", replayed, skipped)
}

func republish(ch *amqp.Channel, routingKey string, delivery amqp.Delivery) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return ch.PublishWithContext(
		ctx,
		exchangeName,
		routingKey,
		false, false,
		amqp.Publishing{
			ContentType:  delivery.ContentType,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         delivery.Body,
		},
	)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}