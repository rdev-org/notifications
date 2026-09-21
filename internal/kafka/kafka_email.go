package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/rdev-org/notifications/internal/constant"
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
	RecipientEmail string            `json:"recipient_email"`
	RecipientName  string            `json:"recipient_name"`
	Subject        string            `json:"subject"`
	Body           string            `json:"body"`
	Meta           map[string]string `json:"meta"`
	SentAt         time.Time         `json:"sent_at"`
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
			if ctx.Err() != nil || isEOFOrClosedErr(err) {
				log.Println("[Kafka Email Consumer] Consumer stopped cleanly.")
				return nil
			}

			log.Printf("[Kafka Email Consumer] Error reading message: %v", err)

			select {
			case <-time.After(1 * time.Second):
			case <-ctx.Done():
				return nil
			}

			continue
		}

		if msg.Topic == "email-notifications" {
			var kp KafkaPayload
			if err := json.Unmarshal(msg.Value, &kp); err != nil {
				log.Printf("[Kafka Email Consumer] Failed to unmarshal Kafka Payload: %v", err)
				continue
			}

			if err := k.routeEvent(kp, kp.Type); err != nil {
				log.Printf("[Kafka Email Consumer] Failed to process event [%s]: %v", kp.Type, err)
			}

			if err := k.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("[Kafka Email Consumer] Failed to commit message offset: %v", err)
			}
		}
	}
}

func (k *KafkaEmailConsumer) routeEvent(kp KafkaPayload, kpType string) error {
	var event EmailEvent
	if err := json.Unmarshal(kp.Payload, &event); err != nil {
		return fmt.Errorf("invalid EmailEvent payload: %w", err)
	}

	var err error
	switch kpType {
	case "LOGIN_EMAIL":
		err = email.SendTemplateEmail(event.RecipientEmail, event.Subject, "login.html", event)
	case "REGISTER_EMAIL":
		event.Meta["ActionUrl"] = fmt.Sprintf("%s?token=%s", constant.RegisterURL, event.Meta["Token"])
		err = email.SendTemplateEmail(event.RecipientEmail, event.Subject, "registration.html", event)
	case "WELCOME_EMAIL":
		event.Meta["ActionUrl"] = constant.WelcomeURL
		err = email.SendTemplateEmail(event.RecipientEmail, event.Subject, "welcome.html", event)
	case "RESET_PASSWORD", "FORGET_PASSWORD":
		err = email.SendEmail(event.RecipientEmail, event.Subject, "Reset your password")
	}

	if err != nil {
		return fmt.Errorf("failed sending email to %s: %w", event.RecipientEmail, err)
	}

	log.Printf("[Kafka Email Consumer] Successfully sent %s email to %s", kp.Type, event.RecipientEmail)
	return nil
}

func (k *KafkaEmailConsumer) Close() error {
	if k.reader != nil {
		return k.reader.Close()
	}
	return nil
}

func isEOFOrClosedErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, "EOF") || strings.Contains(errStr, "use of closed network connection")
}
