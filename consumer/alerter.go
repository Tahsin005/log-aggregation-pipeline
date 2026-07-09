package main

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	alertQueueName = "critical_alerts_queue"
)

var alertBindingKeys = []string{"*.critical", "*.error"}

type Alerter struct {
	channel *amqp.Channel
	queue   amqp.Queue
}

func NewAlerter(conn *amqp.Connection) (*Alerter, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := ch.ExchangeDeclare(exchangeName, exchangeType, true, false, false, false, nil); err != nil {
		ch.Close()
		return nil, err
	}

	queue, err := ch.QueueDeclare(alertQueueName, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		return nil, err
	}

	for _, key := range alertBindingKeys {
		if err := ch.QueueBind(queue.Name, key, exchangeName, false, nil); err != nil {
			ch.Close()
			return nil, err
		}
	}

	if err := ch.Qos(10, 0, false); err != nil {
		ch.Close()
		return nil, err
	}

	return &Alerter{channel: ch, queue: queue}, nil
}

func (a *Alerter) Run(ctx context.Context) error {
	deliveries, err := a.channel.Consume(
		a.queue.Name,
		"alerter",
		false, // manual ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	log.Println("alerter consuming from", a.queue.Name, "bound to", alertBindingKeys)

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down alerter")
			return nil

		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}

			var entry LogEntry
			if err := json.Unmarshal(delivery.Body, &entry); err != nil {
				log.Printf("alerter: failed to unmarshal message, discarding: %v", err)
				delivery.Nack(false, false)
				continue
			}

			raiseAlert(entry, delivery.RoutingKey)
			delivery.Ack(false)
		}
	}
}

func raiseAlert(entry LogEntry, routingKey string) {
	log.Printf("🚨 ALERT [%s] service=%s severity=%s message=%q",
		routingKey, entry.Service, entry.Severity, entry.Message)
}

func (a *Alerter) Close() {
	if a.channel != nil {
		a.channel.Close()
	}
}