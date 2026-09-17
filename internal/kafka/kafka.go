package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/rdev-org/notifications/internal/email"
	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader *kafka.Reader
}

type KafkaPayload struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type EmailEvent struct {
	Recipient string `json:"recipient"`
	Subject   string `json:"subject"`
}

func NewEmailConsumer(reader *kafka.Reader) *KafkaConsumer {
	return &KafkaConsumer{
		reader: reader,
	}
}

func (k *KafkaConsumer) Listen(ctx context.Context) error {
	for {
		msg, err := k.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("[Kafka Consumer] Error reading message: %v", err)
			continue
		}

		var kPayload KafkaPayload
		if err := json.Unmarshal(msg.Value, &kPayload); err != nil {
			log.Printf("[Kafka Consumer] Failed to unmarshal Kafka Payload: %v", err)
			continue
		}

		if err := k.routeEvent(kPayload); err != nil {
			log.Printf("[Kafka Consumer] Failed to process event [%s]: %v", kPayload.Type, err)
		}
	}
}

func (k *KafkaConsumer) routeEvent(kp KafkaPayload) error {
	switch kp.Type {
	case "RESET_PASSWORD", "FORGET_PASSWORD":
		var event EmailEvent
		if err := json.Unmarshal(kp.Payload, &event); err != nil {
			return fmt.Errorf("invalid EmailEvent payload: %w", err)
		}

		if err := email.SendEmail(event.Recipient, event.Subject); err != nil {
			return fmt.Errorf("failed sending email to %s: %w", event.Recipient, err)
		}

		log.Printf("[Kafka Consumer] Successfully sent %s email to %s", kp.Type, event.Recipient)
		return nil

	default:
		return fmt.Errorf("unhandled event type: %s", kp.Type)
	}
}
