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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	defer client.Recover(ctx)

	panic("example panic")
}
