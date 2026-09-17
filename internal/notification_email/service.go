package notification_email

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/rdev-org/notifications/internal/constant"
	k "github.com/rdev-org/notifications/internal/kafka"
	pb "github.com/rdev-org/proto-contracts/pb/notification_email"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NotificationEmailService struct {
	pb.UnimplementedNotificationEmailServiceServer
	kafkaWriter *kafka.Writer
}

func NewNotificationEmailService(kw *kafka.Writer) *NotificationEmailService {
	return &NotificationEmailService{
		kafkaWriter: kw,
	}
}

func (s *NotificationEmailService) RegisterEmail(ctx context.Context, req *pb.RegisterEmailRequest) (*pb.RegisterEmailResponse, error) {
	return &pb.RegisterEmailResponse{
		Message: constant.Registration,
		SentAt:  timestamppb.New(time.Now()),
	}, nil
}

func (s *NotificationEmailService) ResetPasswordEmail(ctx context.Context, req *pb.ResetPasswordEmailRequest) (*pb.ResetPasswordEmailResponse, error) {
	reqEmail := req.GetEmail()
	emailData := k.EmailEvent{
		Recipient: reqEmail.GetRecipient(),
		Subject:   reqEmail.GetSubject(),
	}

	payloadBytes, err := json.Marshal(emailData)
	if err != nil {
		log.Printf("[NotificationEmail] service - failed to marshal email event: %v", err)
		return nil, status.Error(codes.Internal, "failed to process request")
	}

	kp := k.KafkaPayload{
		Type:    "RESET_PASSWORD",
		Payload: payloadBytes,
	}

	msgBytes, err := json.Marshal(kp)
	if err != nil {
		log.Printf("[NotificationEmail] service - failed to marshal kafka payload: %v", err)
		return nil, status.Error(codes.Internal, "failed to process request")
	}

	if err := s.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Topic: "notification-emails",
		Key:   []byte(reqEmail.GetRecipient()),
		Value: msgBytes,
	}); err != nil {
		log.Printf("[NotificationEmail] failed to write message to kafka: %v", err)
		return nil, status.Error(codes.Internal, "failed to queue email message")
	}

	return &pb.ResetPasswordEmailResponse{
		Message: constant.ResetPassword,
		SentAt:  timestamppb.New(time.Now()),
	}, nil
}
