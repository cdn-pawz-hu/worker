package storage

import (
	"context"
	"fmt"

	"cdn-worker/internal/config"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioClient struct {
	client    *minio.Client
	rawBucket string
	encBucket string
}

func NewMinioClient(cfg *config.Config) (*MinioClient, error) {
	client, err := minio.New(cfg.MinIO.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIO.AccessKey, cfg.MinIO.SecretKey, ""),
		Secure: cfg.MinIO.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init minio sdk: %w", err)
	}

	return &MinioClient{
		client:    client,
		rawBucket: cfg.MinIO.RawBucket,
		encBucket: cfg.MinIO.EncBucket,
	}, nil
}

// DownloadRawVideo pulls the source file from your incoming bucket and saves it locally
func (m *MinioClient) DownloadRawVideo(ctx context.Context, objectKey, destinationPath string) error {
	err := m.client.FGetObject(ctx, m.rawBucket, objectKey, destinationPath, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("minio download failed: %w", err)
	}
	return nil
}

// UploadEncodedVideo pushes the processed file up to your distribution bucket
func (m *MinioClient) UploadEncodedVideo(ctx context.Context, objectKey, sourcePath string) error {
	opts := minio.PutObjectOptions{
		ContentType: "video/mp4",
	}
	_, err := m.client.FPutObject(ctx, m.encBucket, objectKey, sourcePath, opts)
	if err != nil {
		return fmt.Errorf("minio upload failed: %w", err)
	}
	return nil
}
