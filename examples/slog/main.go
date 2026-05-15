package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	auralogs "github.com/auralogs-ai/auralogs-go"
)

func main() {
	client, err := auralogs.Init(auralogs.Config{
		APIKey:      os.Getenv("AURALOG_API_KEY"),
		Environment: "production",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = auralogs.Shutdown(ctx)
	}()

	logger := slog.New(auralogs.NewSlogHandler(client, slog.LevelInfo))
	logger.WithGroup("request").Error("payment failed", "order_id", "abc")
}
