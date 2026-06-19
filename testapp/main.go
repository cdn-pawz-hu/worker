package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Config mirrors the worker's configuration structure
type Config struct {
	RabbitMQ struct {
		URL       string `env:"RABBITMQ_URL" env-required:"true"`
		QueueName string `env:"QUEUE_NAME" env-default:"video_encoding_queue"`
	}
	MinIO struct {
		Endpoint  string `env:"MINIO_ENDPOINT" env-required:"true"`
		AccessKey string `env:"MINIO_ACCESS_KEY" env-required:"true"`
		SecretKey string `env:"MINIO_SECRET_KEY" env-required:"true"`
		UseSSL    bool   `env:"MINIO_USE_SSL" env-default:"false"`
		RawBucket string `env:"RAW_BUCKET" env-required:"true"`
	}
}

type VideoTaskPayload struct {
	VideoID   string `json:"video_id"`
	ObjectKey string `json:"object_key"`
}

func main() {
	// 1. Parse CLI arguments
	filePath := flag.String("file", "", "Path to the local .mp4 video file to upload")
	flag.Parse()

	if *filePath == "" {
		log.Fatal("Error: You must provide a file path using the -file flag.")
	}

	// Verify file exists
	if _, err := os.Stat(*filePath); os.IsNotExist(err) {
		log.Fatalf("Error: File does not exist at path: %s", *filePath)
	}

	// 2. Load Configuration
	var cfg Config
	if err := cleanenv.ReadConfig(".env", &cfg); err != nil {
		log.Fatalf("Config initialization failed: %v", err)
	}

	ctx := context.Background()
	videoID := uuid.New().String()
	objectKey := fmt.Sprintf("raw/%s%s", videoID, filepath.Ext(*filePath))

	// 3. Upload to MinIO
	log.Printf("Connecting to MinIO at %s...", cfg.MinIO.Endpoint)
	minioClient, err := minio.New(cfg.MinIO.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIO.AccessKey, cfg.MinIO.SecretKey, ""),
		Secure: cfg.MinIO.UseSSL,
	})
	if err != nil {
		log.Fatalf("Failed to init MinIO client: %v", err)
	}

	log.Printf("Uploading %s to bucket %s...", *filePath, cfg.MinIO.RawBucket)
	_, err = minioClient.FPutObject(ctx, cfg.MinIO.RawBucket, objectKey, *filePath, minio.PutObjectOptions{
		ContentType: "video/mp4",
	})
	if err != nil {
		log.Fatalf("Upload failed: %v", err)
	}
	log.Println("Upload complete.")

	// 4. Publish to RabbitMQ
	log.Printf("Connecting to RabbitMQ at %s...", cfg.RabbitMQ.URL)
	conn, err := amqp.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	payload := VideoTaskPayload{
		VideoID:   videoID,
		ObjectKey: objectKey,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("Failed to marshal JSON payload: %v", err)
	}

	err = ch.PublishWithContext(ctx,
		"",                     // exchange
		cfg.RabbitMQ.QueueName, // routing key
		false,                  // mandatory
		false,                  // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // Ensure message survives broker restarts
			Body:         body,
		})
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}

	log.Printf("Successfully published job to queue. Video ID: %s", videoID)
}
