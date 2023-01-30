package gcs 

import(
	"context"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
	"github.com/pkg/errors"
)

type GCSClient interface{
	WriteData([]byte) (error)
	Close() (error)
}

type gcsClient struct {
	client    *storage.Client
	writer    *storage.Writer
}

func newGCSClient(ctx context.Context,serviceAccountJSON []byte, bucketname string, filepath string)(GCSClient, error){
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON(serviceAccountJSON))
	if err != nil {
		return nil, errors.Wrap(err, "error in creating client")
	}

	writer := client.Bucket(bucketname).Object(filepath).NewWriter(ctx)

	return &gcsClient{
		client: client,
		writer: writer,
	},nil
}

func (c *gcsClient) WriteData(jsonBytes []byte) (error){
	if _, err := c.writer.Write(jsonBytes); err!=nil{
		return errors.Wrap(err,"error in writing json data to an object")
	}

	return nil
}

func (c *gcsClient) Close()(error){
	if err := c.writer.Close(); err != nil {
		return errors.Wrap(err, "error closing the writer")
	} 

	return nil
}
