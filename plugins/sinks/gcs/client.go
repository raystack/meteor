package gcs

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

type Writer interface {
	WriteData([]byte) error
	Close() error
}

type gcsWriter struct {
	writer *storage.Writer
}

func newWriter(ctx context.Context, serviceAccountJSON []byte, bucketname, filepath string) (*gcsWriter, error) {
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON(serviceAccountJSON))
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}

	writer := client.Bucket(bucketname).Object(filepath).NewWriter(ctx)

	return &gcsWriter{
		writer: writer,
	}, nil
}

func (c *gcsWriter) WriteData(data []byte) error {
	if _, err := c.writer.Write(data); err != nil {
		return fmt.Errorf("write data to an object: %w", err)
	}

	return nil
}

func (c *gcsWriter) Close() error {
	return c.writer.Close()
}
