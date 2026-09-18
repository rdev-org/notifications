package container

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pbEmail "github.com/rdev-org/proto-contracts/pb/notification_email"

	"github.com/rdev-org/notifications/internal/config"
	k "github.com/rdev-org/notifications/internal/kafka"
	"github.com/rdev-org/notifications/internal/notification_email"
	"github.com/segmentio/kafka-go"
	"github.com/uptrace/bun"
)

type Container struct {
	db            *bun.DB
	kafkaWriter   *kafka.Writer
	emailConsumer *k.KafkaEmailConsumer
	inAppConsumer *k.KafkaInAppConsumer
}

func NewContainer(db *bun.DB, cfgk *config.KafkaConfig) *Container {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfgk.Brokers...),
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	emailReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfgk.Brokers,
		Topic:       "email-notifications",
		GroupID:     "email-workers",
		StartOffset: kafka.FirstOffset,
	})

	inAppReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfgk.Brokers,
		Topic:       "in-app-notifications",
		GroupID:     "inapp-workers",
		StartOffset: kafka.FirstOffset,
	})

	return &Container{
		db:            db,
		kafkaWriter:   writer,
		emailConsumer: k.NewEmailConsumer(emailReader),
		inAppConsumer: k.NewInAppConsumer(inAppReader),
	}
}

func (c *Container) Initialize() *grpc.Server {
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(unaryLogInterceptor),
	)

	c.RegisterNotificationEmailService(grpcServer)

	reflection.Register(grpcServer)

	return grpcServer
}

func (c *Container) Close() {
	if c.kafkaWriter != nil {
		if err := c.kafkaWriter.Close(); err != nil {
			log.Printf("error closing kakfka writer: %v", err)
		}
	}
	if c.emailConsumer != nil {
		if err := c.emailConsumer.Close(); err != nil {
			log.Printf("error closing kakfka email reader: %v", err)
		}
	}
	if c.inAppConsumer != nil {
		if err := c.inAppConsumer.Close(); err != nil {
			log.Printf("error closing kakfka in-app reader: %v", err)
		}
	}
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			log.Printf("error closing database conn: %v", err)
		}
	}
}

func (c *Container) StartKafkaConsumer(ctx context.Context) {
	go func() {
		log.Println("[Kafka] Email worker started listening...")
		if err := c.emailConsumer.Listen(ctx); err != nil {
			log.Printf("[Kafka] Email consumer error: %v", err)
		}
	}()

	go func() {
		log.Println("[Kafka] In-App worker started listening...")
		if err := c.inAppConsumer.Listen(ctx); err != nil {
			log.Printf("[Kafka] In-App consumer error: %v", err)
		}
	}()
}

func (c *Container) RegisterNotificationEmailService(grpcServer *grpc.Server) {
	s := notification_email.NewNotificationEmailService(c.kafkaWriter)
	pbEmail.RegisterNotificationEmailServiceServer(grpcServer, s)
}

func unaryLogInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()

	resp, err := handler(ctx, req)

	duration := time.Since(start)
	st, _ := status.FromError(err)

	log.Printf("[gRPC] Method: %s | Duration: %v | Status: %v", info.FullMethod, duration, st.Code())

	return resp, err
}
