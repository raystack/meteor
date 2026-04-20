package client

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Writer interface {
	WriteData([]byte) error
	Close() error
}

type S3Writer struct {
	ctx    context.Context
	client *s3.Client
	bucket string
	key    string
	buf    bytes.Buffer
}

func NewWriter(ctx context.Context, bucket, key, region, accessKeyID, secretAccessKey, endpoint string) (*S3Writer, error) {
	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(region))

	if accessKeyID != "" && secretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(cfg, s3Opts...)

	return &S3Writer{
		ctx:    ctx,
		client: client,
		bucket: bucket,
		key:    key,
	}, nil
}

func (w *S3Writer) WriteData(data []byte) error {
	if _, err := w.buf.Write(data); err != nil {
		return fmt.Errorf("write data to buffer: %w", err)
	}
	return nil
}

func (w *S3Writer) Close() error {
	_, err := w.client.PutObject(w.ctx, &s3.PutObjectInput{
		Bucket:      aws.String(w.bucket),
		Key:         aws.String(w.key),
		Body:        bytes.NewReader(w.buf.Bytes()),
		ContentType: aws.String("application/x-ndjson"),
	})
	if err != nil {
		return fmt.Errorf("upload object to S3: %w", err)
	}
	return nil
}
