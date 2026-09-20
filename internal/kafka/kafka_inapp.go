package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaInAppConsumer struct {
	reader *kafka.Reader
}

func NewInAppConsumer(reader *kafka.Reader) *KafkaInAppConsumer {
	return &KafkaInAppConsumer{
		reader: reader,
	}
}

func (k *KafkaInAppConsumer) Listen(ctx context.Context) error {
	for {
		msg, err := k.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || isEOFOrClosedErr(err) {
				log.Println("[Kafka InApp Consumer] Consumer stopped cleanly.")
				return nil
			}

			log.Printf("[Kafka InApp Consumer] Error reading message: %v", err)

			select {
			case <-time.After(1 * time.Second):
			case <-ctx.Done():
				return nil
			}

			continue
		}

		fmt.Printf("\n[InApp Consumer] KAFKA TOPIC: %s\n\n", msg.Topic)
	}
}

func (k *KafkaInAppConsumer) Close() error {
	if k.reader != nil {
		return k.reader.Close()
	}
	return nil
}
