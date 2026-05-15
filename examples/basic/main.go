package main

import (
	"context"
	"log"
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

	client.Info("user signed in", auralogs.Metadata{"user_id": "123"})
	client.Error("payment failed", auralogs.Metadata{"order_id": "abc"})
}
