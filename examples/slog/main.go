package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	auralog "github.com/auralog-ai/auralog-go"
)

func main() {
	client, err := auralog.Init(auralog.Config{
		APIKey:      os.Getenv("AURALOG_API_KEY"),
		Environment: "production",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = auralog.Shutdown(ctx)
	}()

	logger := slog.New(auralog.NewSlogHandler(client, slog.LevelInfo))
	logger.WithGroup("request").Error("payment failed", "order_id", "abc")
}
