package service

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/rdev-org/notifications/internal/constant"
	k "github.com/rdev-org/notifications/internal/kafka"
	pb "github.com/rdev-org/proto-contracts/pb/notification"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NotificationEmailService struct {
	pb.UnimplementedNotificationServiceServer
	kafkaWriter *kafka.Writer
}

func NewNotificationEmailService(kw *kafka.Writer) *NotificationEmailService {
	return &NotificationEmailService{
		kafkaWriter: kw,
	}
}

func (s *NotificationEmailService) SendEmail(ctx context.Context, req *pb.SendEmailRequest) (*pb.SendEmailResponse, error) {
	reqEmail := req.GetEmail()
	reqCtx := req.GetRequestContext()

	emailEvent := k.EmailEvent{
		RecipientEmail: reqEmail.GetRecipient(),
		RecipientName:  reqEmail.GetName(),
		Subject:        reqEmail.GetSubject(),
		Body:           reqEmail.GetBody(),
		Meta: map[string]string{
			"IpAddress": reqCtx.GetIpAddress(),
			"UserAgent": reqCtx.GetUserAgent(),
		},
		SentAt: time.Now(),
	}

	if len(reqEmail.GetMeta()) > 0 && reqEmail.GetMeta()["Token"] != "" {
		emailEvent.Meta["Token"] = reqEmail.GetMeta()["Token"]
	}

	payloadBytes, err := json.Marshal(emailEvent)
	if err != nil {
		log.Printf("[NotificationEmail] service - failed to marshal email event reset send email: %v", err)
		return nil, status.Error(codes.Internal, "failed to process request")
	}

	msgBytes, err := json.Marshal(k.KafkaPayload{
		Type:    req.Module,
		Payload: payloadBytes,
	})
	if err != nil {
		log.Printf("[NotificationEmail] service - failed to marshal kafka payload reset send email: %v", err)
		return nil, status.Error(codes.Internal, "failed to process request")
	}

	if err := s.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Topic: "email-notifications",
		Key:   []byte(reqEmail.GetRecipient()),
		Value: msgBytes,
	}); err != nil {
		log.Printf("[NotificationEmail] failed to write message to kafka reset send email: %v", err)
		return nil, status.Error(codes.Internal, "failed to queue email message")
	}

	return &pb.SendEmailResponse{
		Message: constant.Email,
		SentAt:  timestamppb.New(time.Now()),
	}, nil
}

func (s *NotificationEmailService) RegisterEmail(ctx context.Context, req *pb.RegisterEmailRequest) (*pb.RegisterEmailResponse, error) {
	return &pb.RegisterEmailResponse{
		Message: constant.Registration,
		SentAt:  timestamppb.New(time.Now()),
	}, nil
}

func (s *NotificationEmailService) ResetPasswordEmail(ctx context.Context, req *pb.ResetPasswordEmailRequest) (*pb.ResetPasswordEmailResponse, error) {
	reqEmail := req.GetEmail()

	payloadBytes, err := json.Marshal(k.EmailEvent{
		RecipientEmail: reqEmail.GetRecipient(),
		RecipientName:  reqEmail.GetName(),
		Subject:        reqEmail.GetSubject(),
	})
	if err != nil {
		log.Printf("[NotificationEmail] service - failed to marshal email event reset password: %v", err)
		return nil, status.Error(codes.Internal, "failed to process request")
	}

	msgBytes, err := json.Marshal(k.KafkaPayload{
		Type:    "RESET_PASSWORD",
		Payload: payloadBytes,
	})
	if err != nil {
		log.Printf("[NotificationEmail] service - failed to marshal kafka payload reset password: %v", err)
		return nil, status.Error(codes.Internal, "failed to process request")
	}

	if err := s.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Topic: "email-notifications",
		Key:   []byte(reqEmail.GetRecipient()),
		Value: msgBytes,
	}); err != nil {
		log.Printf("[NotificationEmail] failed to write message to kafka reset password: %v", err)
		return nil, status.Error(codes.Internal, "failed to queue email message")
	}

	return &pb.ResetPasswordEmailResponse{
		Message: constant.ResetPassword,
		SentAt:  timestamppb.New(time.Now()),
	}, nil
}
