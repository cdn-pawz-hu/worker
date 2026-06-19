package config

import (
	"log"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	RabbitMQ struct {
		URL       string `env:"RABBITMQ_URL" env-required:"true"`
		QueueName string `env:"QUEUE_NAME" env-default:"video_encoding_queue"`
	}
	MinIO struct {
		Endpoint  string `env:"MINIO_ENDPOINT" env-required:"true"`
		AccessKey string `env:"MINIO_ACCESS_KEY" env-reuquired:"true"`
		SecretKey string `env:"MINIO_SECRET_KEY" env-reuquired:"true"`
		UseSSL    bool   `env:"MINIO_USE_SSL" env-default:"false"`
		RawBucket string `env:"RAW_BUCKET" env-required:"true"`
		EncBucket string `env:"ENCODED_BUCKET" env-required:"true"`
	}
}

func Load() *Config {
	var cfg Config

	if err := cleanenv.ReadConfig(".env", &cfg); err != nil {
		log.Fatalf("Config initialization failed: %v", err)
	}
	return &cfg
}
