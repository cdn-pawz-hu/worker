package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"cdn-worker/internal/config"
	"cdn-worker/internal/encoder"
	"cdn-worker/internal/queue"
	"cdn-worker/internal/storage"
)

func main() {
	// 1. Load Configuration
	cfg := config.Load()
	log.Println("Configuration loaded successfully")

	// 2. Initialize Storage Client (MinIO)
	storageClient, err := storage.NewMinioClient(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	// 3. Initialize RabbitMQ Connection
	rmqClient, err := queue.NewRabbitMQClient(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize queue: %v", err)
	}
	defer rmqClient.Close()

	// 4. Initialize Encoder Engine
	encoderEngine := encoder.NewFFmpegEncoder()

	// 5. Start Worker Loop
	// Pass the dependencies into the consumer
	go func() {
		err := rmqClient.StartConsuming(storageClient, encoderEngine)
		if err != nil {
			log.Fatalf("Consumer crashed: %v", err)
		}
	}()

	// 6. Graceful Shutdown
	// Prevents the pod from killing FFmpeg midway through an encode when scaling down
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)
	<-stopChan

	log.Println("Shutting down worker gracefully...")
}
