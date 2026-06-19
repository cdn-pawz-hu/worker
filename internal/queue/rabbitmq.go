package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"cdn-worker/internal/config"
	"cdn-worker/internal/encoder"
	"cdn-worker/internal/storage"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQClient struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
}

type VideoTaskPayload struct {
	VideoID   string `json:"video_id"`
	ObjectKey string `json:"object_key"`
}

func NewRabbitMQClient(cfg *config.Config) (*RabbitMQClient, error) {
	conn, err := amqp.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq broker: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open AMQP channel: %w", err)
	}

	// Declare durable queue to withstand broker restarts
	q, err := ch.QueueDeclare(cfg.RabbitMQ.QueueName, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare target queue: %w", err)
	}

	// Crucial: Set prefetch count to 1 for precise scaling via KEDA
	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to apply QoS prefetch policy: %w", err)
	}

	return &RabbitMQClient{
		conn:      conn,
		channel:   ch,
		queueName: q.Name,
	}, nil
}

func (r *RabbitMQClient) Close() {
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
}

func (r *RabbitMQClient) StartConsuming(store *storage.MinioClient, enc *encoder.FFmpegEncoder) error {
	msgs, err := r.channel.Consume(r.queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to stream delivery channel: %w", err)
	}

	for delivery := range msgs {
		var payload VideoTaskPayload
		if err := json.Unmarshal(delivery.Body, &payload); err != nil {
			log.Printf("[Queue] Corrupt payload schema rejected: %v", err)
			delivery.Nack(false, false) // Drop structural junk immediately
			continue
		}

		log.Printf("[Queue] Worker locked onto job. Processing video ID: %s", payload.VideoID)

		if err := r.executeJob(payload, store, enc); err != nil {
			log.Printf("[Queue] Execution pipeline failed for task %s: %v", payload.VideoID, err)
			// Requeue item so an available node can retry execution
			delivery.Nack(false, true)
		} else {
			log.Printf("[Queue] Task finalized successfully. Acknowledging video ID: %s", payload.VideoID)
			delivery.Ack(false)
		}
	}

	return nil
}

func (r *RabbitMQClient) executeJob(task VideoTaskPayload, store *storage.MinioClient, enc *encoder.FFmpegEncoder) error {
	ctx := context.Background()
	tmpDir := os.TempDir()

	localInput := filepath.Join(tmpDir, fmt.Sprintf("src_%s.mp4", task.VideoID))
	localOutput := filepath.Join(tmpDir, fmt.Sprintf("out_%s.mp4", task.VideoID))

	// Ensure structural cleanup to protect bare-metal scratch spaces
	defer os.Remove(localInput)
	defer os.Remove(localOutput)

	// Step 1: Ingest
	if err := store.DownloadRawVideo(ctx, task.ObjectKey, localInput); err != nil {
		return fmt.Errorf("pipeline step (ingest) failed: %w", err)
	}

	// Step 2: Render
	if err := enc.TranscodeTo1080pH264(localInput, localOutput); err != nil {
		return fmt.Errorf("pipeline step (render) failed: %w", err)
	}

	// Step 3: Egress
	targetFilename := fmt.Sprintf("processed/%s.mp4", task.VideoID)
	if err := store.UploadEncodedVideo(ctx, targetFilename, localOutput); err != nil {
		return fmt.Errorf("pipeline step (egress) failed: %w", err)
	}

	return nil
}
