package email

import (
	"fmt"
	"log"
	"net/smtp"
	"strings"

	"github.com/rdev-org/notifications/internal/config"
)

type Email struct {
	//
}

func SendEmail(to, subject string) error {
	appConfig, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config %v", err)
	}

	body := "This is a test message."

	header := make(map[string]string)
	header["From"] = appConfig.SMTP.From
	header["To"] = to
	header["Subject"] = subject
	header["MIME-Version"] = "1.0"
	header["Content-Type"] = "text/plain; charset=\"utf-8\""

	var message strings.Builder
	for k, v := range header {
		fmt.Fprintf(&message, "%s: %s\r\n", k, v)
	}
	message.WriteString("\r\n")
	message.WriteString(body)

	addr := fmt.Sprintf("%s:%s", appConfig.SMTP.Host, appConfig.SMTP.Port)
	auth := smtp.PlainAuth(appConfig.SMTP.From, appConfig.SMTP.User, appConfig.SMTP.Pass, appConfig.SMTP.Host)

	err = smtp.SendMail(addr, auth, appConfig.SMTP.From, []string{to}, []byte(message.String()))
	if err != nil {
		log.Printf("[NotificationEmail]service - failed to send email %v", err)
		return err
	}

	return nil
}
