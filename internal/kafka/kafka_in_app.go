package kafka

import "github.com/segmentio/kafka-go"

func NewInAppConsumer(reader *kafka.Reader) *KafkaConsumer {
	return &KafkaConsumer{
		reader: reader,
	}
}
