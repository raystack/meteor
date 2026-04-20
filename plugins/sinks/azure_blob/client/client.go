package client

import (
	"bytes"
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type Writer interface {
	WriteData([]byte) error
	Close() error
}

type AzureBlobWriter struct {
	ctx           context.Context
	client        *azblob.Client
	containerName string
	blobName      string
	buf           bytes.Buffer
}

func NewWriter(ctx context.Context, storageAccountURL, containerName, blobName, accountKey, connectionString string) (*AzureBlobWriter, error) {
	var client *azblob.Client
	var err error

	switch {
	case connectionString != "":
		client, err = azblob.NewClientFromConnectionString(connectionString, nil)
	case accountKey != "":
		cred, credErr := azblob.NewSharedKeyCredential(extractAccountName(storageAccountURL), accountKey)
		if credErr != nil {
			return nil, fmt.Errorf("create shared key credential: %w", credErr)
		}
		client, err = azblob.NewClientWithSharedKeyCredential(storageAccountURL, cred, nil)
	default:
		return nil, fmt.Errorf("credentials are not specified: provide account_key or connection_string")
	}

	if err != nil {
		return nil, fmt.Errorf("create Azure Blob client: %w", err)
	}

	return &AzureBlobWriter{
		ctx:           ctx,
		client:        client,
		containerName: containerName,
		blobName:      blobName,
	}, nil
}

// extractAccountName extracts the storage account name from a URL like
// https://<account>.blob.core.windows.net
func extractAccountName(storageAccountURL string) string {
	// Simple extraction: strip scheme, take first dot-separated segment.
	url := storageAccountURL
	for _, prefix := range []string{"https://", "http://"} {
		if len(url) > len(prefix) && url[:len(prefix)] == prefix {
			url = url[len(prefix):]
			break
		}
	}
	for i, c := range url {
		if c == '.' {
			return url[:i]
		}
	}
	return url
}

func (w *AzureBlobWriter) WriteData(data []byte) error {
	if _, err := w.buf.Write(data); err != nil {
		return fmt.Errorf("write data to buffer: %w", err)
	}
	return nil
}

func (w *AzureBlobWriter) Close() error {
	_, err := w.client.UploadBuffer(w.ctx, w.containerName, w.blobName, w.buf.Bytes(), &azblob.UploadBufferOptions{})
	if err != nil {
		return fmt.Errorf("upload blob to Azure Blob Storage: %w", err)
	}
	return nil
}
