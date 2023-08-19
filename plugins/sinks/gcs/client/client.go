package client

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
)

type Writer interface {
	WriteData([]byte) error
	Close() error
}

type GCSWriter struct {
	writer *storage.Writer
}

func NewWriter(ctx context.Context, serviceAccountJSON []byte, bucketname string, filepath string) (*GCSWriter, error) {
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON(serviceAccountJSON))
	if err != nil {
		return nil, errors.Wrap(err, "error in creating client")
	}

	writer := client.Bucket(bucketname).Object(filepath).NewWriter(ctx)

	return &GCSWriter{
		writer: writer,
	}, nil
}

func (c *GCSWriter) WriteData(data []byte) error {
	if _, err := c.writer.Write(data); err != nil {
		return errors.Wrap(err, "error in writing data to an object")
	}

	return nil
}

func (c *GCSWriter) Close() error {
	return c.writer.Close()
}
