package main

import (
	"context"
	"log"

	"github.com/rdev-org/helpers/app"
	"github.com/rdev-org/notifications/internal/config"
	"github.com/rdev-org/notifications/internal/container"
)

func main() {
	appConfig, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config %v", err)
	}

	db := config.ConnectDB(appConfig.Database)
	c := container.NewContainer(db, &appConfig.Kafka)

	ctx, cancel := context.WithCancel(context.Background())
	c.StartKafkaConsumer(ctx)

	grpcServer := c.Initialize(app.NewGRPCServer())

	srvCfg := app.ServerConfig{
		Name: "Notification",
		Host: appConfig.Server.Host,
		Port: appConfig.Server.Port,
	}

	app.RunGRPC(srvCfg, grpcServer, func() {
		cancel()
		c.Close()
	})
}
