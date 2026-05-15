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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	defer client.Recover(ctx)

	panic("example panic")
}
