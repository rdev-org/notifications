package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/rdev-org/notifications/internal/email"
	"github.com/segmentio/kafka-go"
)

type KafkaEmailConsumer struct {
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

func NewEmailConsumer(reader *kafka.Reader) *KafkaEmailConsumer {
	return &KafkaEmailConsumer{
		reader: reader,
	}
}

func (k *KafkaEmailConsumer) Listen(ctx context.Context) error {
	for {
		msg, err := k.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("[Kafka Email Consumer] Error reading message: %v", err)
			continue
		}

		fmt.Printf("\n[Email Consumer] KAFKA TOPIC: %s\n\n", msg.Topic)

		if msg.Topic == "notification-emails" {
			var kp KafkaPayload
			if err := json.Unmarshal(msg.Value, &kp); err != nil {
				log.Printf("[Kafka Email Consumer] Failed to unmarshal Kafka Payload: %v", err)
				continue
			}

			if err := k.routeEvent(kp); err != nil {
				log.Printf("[Kafka Email Consumer] Failed to process event [%s]: %v", kp.Type, err)
			}

			if err := k.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("[Kafka Email Consumer] Failed to commit message offset: %v", err)
			}
		}
	}
}

func (k *KafkaEmailConsumer) routeEvent(kp KafkaPayload) error {
	switch kp.Type {
	case "RESET_PASSWORD", "FORGET_PASSWORD":
		var event EmailEvent
		if err := json.Unmarshal(kp.Payload, &event); err != nil {
			return fmt.Errorf("invalid EmailEvent payload: %w", err)
		}

		if err := email.SendEmail(event.Recipient, event.Subject); err != nil {
			return fmt.Errorf("failed sending email to %s: %w", event.Recipient, err)
		}

		log.Printf("[Kafka Email Consumer] Successfully sent %s email to %s", kp.Type, event.Recipient)
		return nil

	default:
		return fmt.Errorf("unhandled event type: %s", kp.Type)
	}
}

func (k *KafkaEmailConsumer) Close() error {
	if k.reader != nil {
		return k.reader.Close()
	}
	return nil
}
