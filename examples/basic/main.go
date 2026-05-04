package main

import (
	"context"
	"log"
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

	client.Info("user signed in", auralog.Metadata{"user_id": "123"})
	client.Error("payment failed", auralog.Metadata{"order_id": "abc"})
}
