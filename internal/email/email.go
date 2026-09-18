package email

import (
	"bytes"
	"fmt"
	"html/template"
	"log"
	"net/smtp"
	"strings"

	"github.com/rdev-org/notifications/internal/config"
	"github.com/rdev-org/notifications/internal/templates"
)

func SendEmail(to, subject string, body string) error {
	appConfig, err := config.LoadConfig("config.yaml")
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	header := make(map[string]string)
	header["From"] = appConfig.SMTP.From
	header["To"] = to
	header["Subject"] = subject
	header["MIME-Version"] = "1.0"
	// Set Content-Type to HTML so the rendered template displays correctly
	header["Content-Type"] = "text/html; charset=\"utf-8\""

	var message strings.Builder
	for k, v := range header {
		fmt.Fprintf(&message, "%s: %s\r\n", k, v)
	}
	message.WriteString("\r\n")
	message.WriteString(body)

	addr := fmt.Sprintf("%s:%s", appConfig.SMTP.Host, appConfig.SMTP.Port)
	auth := smtp.PlainAuth("", appConfig.SMTP.User, appConfig.SMTP.Pass, appConfig.SMTP.Host)

	err = smtp.SendMail(addr, auth, appConfig.SMTP.From, []string{to}, []byte(message.String()))
	if err != nil {
		log.Printf("[NotificationEmail] service - failed to send email: %v", err)
		return err
	}

	return nil
}

func SendTemplateEmail(to string, subject string, templateName string, data any) error {
	tmpl, err := template.ParseFS(templates.FS, templateName)
	if err != nil {
		return fmt.Errorf("failed to parse template %s: %w", templateName, err)
	}

	var body bytes.Buffer
	if err := tmpl.Execute(&body, data); err != nil {
		return fmt.Errorf("failed to execute template %s: %w", templateName, err)
	}

	return SendEmail(to, subject, body.String())
}
