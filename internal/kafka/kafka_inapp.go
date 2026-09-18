package kafka

import (
	"context"
	"fmt"
	"log"

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
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("[Kafka InApp Consumer] Error reading message: %v", err)
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
