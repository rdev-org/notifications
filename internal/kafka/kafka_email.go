package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

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
	Recipient string            `json:"recipient"`
	Subject   string            `json:"subject"`
	Body      string            `json:"body"`
	Meta      map[string]string `json:"meta"`
	SentAt    time.Time         `json:"sent_at"`
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

		if msg.Topic == "email-notifications" {
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
		return k.parseEmail(kp, kp.Type)

	case "LOGIN_EMAIL":
		return k.parseEmail(kp, "LOGIN_EMAIL")

	default:
		return fmt.Errorf("unhandled event type: %s", kp.Type)
	}
}
func (k *KafkaEmailConsumer) parseEmail(kp KafkaPayload, kpType string) error {
	var event EmailEvent
	if err := json.Unmarshal(kp.Payload, &event); err != nil {
		return fmt.Errorf("invalid EmailEvent payload: %w", err)
	}

	var err error
	switch kpType {
	case "LOGIN_EMAIL":
		err = email.SendTemplateEmail(event.Recipient, event.Subject, "login.html", event)
	case "RESET_PASSWORD", "FORGET_PASSWORD":
		err = email.SendEmail(event.Recipient, event.Subject, "Reset your password")
	}

	if err != nil {
		return fmt.Errorf("failed sending email to %s: %w", event.Recipient, err)
	}

	log.Printf("[Kafka Email Consumer] Successfully sent %s email to %s", kp.Type, event.Recipient)
	return nil
}

func (k *KafkaEmailConsumer) Close() error {
	if k.reader != nil {
		return k.reader.Close()
	}
	return nil
}
