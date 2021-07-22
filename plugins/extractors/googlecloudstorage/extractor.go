package googlecloudstorage

import (
	"context"
	"errors"
	"fmt"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"

	"cloud.google.com/go/storage"
	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/utils"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Config struct {
	ProjectID          string `mapstructure:"project_id" validate:"required"`
	ServiceAccountJSON string `mapstructure:"service_account_json"`
}

type Extractor struct {
	logger plugins.Logger
}

func New(logger plugins.Logger) extractor.BucketExtractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []meta.Bucket, err error) {
	e.logger.Info("extracting kafka metadata...")
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return result, extractor.InvalidConfigError{}
	}
	err = e.validateConfig(config)
	if err != nil {
		return
	}

	ctx := context.Background()
	client, err := e.createClient(ctx, config)
	if err != nil {
		return
	}
	result, err = e.getMetadata(ctx, client, config.ProjectID)
	if err != nil {
		return
	}

	return
}

func (e *Extractor) getMetadata(ctx context.Context, client *storage.Client, projectID string) ([]meta.Bucket, error) {
	it := client.Buckets(ctx, projectID)
	var results []meta.Bucket

	bucket, err := it.Next()
	for err == nil {
		if err != nil {
			return nil, err
		}
		blobs, err := e.getBlobs(ctx, bucket.Name, client, projectID)
		if err != nil {
			return nil, err
		}
		results = append(results, e.mapBucket(bucket, projectID, blobs))
		bucket, err = it.Next()
	}
	if err == iterator.Done {
		err = nil
	}

	return results, nil
}

func (e *Extractor) getBlobs(ctx context.Context, bucketName string, client *storage.Client, projectID string) ([]meta.Blob, error) {
	it := client.Bucket(bucketName).Objects(ctx, nil)
	var blobs []meta.Blob

	object, err := it.Next()
	for err == nil {
		blobs = append(blobs, e.mapObject(object, projectID))
		object, err = it.Next()
	}
	if err == iterator.Done {
		err = nil
	}

	return blobs, err
}

func (e *Extractor) mapBucket(b *storage.BucketAttrs, projectID string, blobs []meta.Blob) meta.Bucket {
	return meta.Bucket{
		Urn:          fmt.Sprintf("%s/%s", projectID, b.Name),
		BucketName:   b.Name,
		Location:     b.Location,
		LocationType: b.LocationType,
		StorageClass: b.StorageClass,
		Blobs:        blobs,
		Source:       "google cloud storage",
		//TODO
		//Timestamps   : &common.Timestamp{
		//	CreatedAt: &timestamp.Timestamp{},
		//},
		//Tags: &facets.Tags{},
	}
}

func (e *Extractor) mapObject(blob *storage.ObjectAttrs, projectID string) meta.Blob {
	return meta.Blob{
		Urn:  fmt.Sprintf("%s/%s/%s", projectID, blob.Bucket, blob.Name),
		Name: blob.Name,
		Size: blob.Size,
		//DeletedAt: ,
		//ExpiredAt
		Ownership: &facets.Ownership{
			Owners: []*facets.Owner{
				{Name: blob.Owner},
			},
		},
		//Tags
		//Timestamps
	}
}

func (e *Extractor) createClient(ctx context.Context, config Config) (*storage.Client, error) {
	if config.ServiceAccountJSON == "" {
		e.logger.Info("credentials are not specified, creating google cloud storage client using Default Credentials...")
		return storage.NewClient(ctx)
	}

	return storage.NewClient(ctx, option.WithCredentialsJSON([]byte(config.ServiceAccountJSON)))
}

func (e *Extractor) getConfig(configMap map[string]interface{}) (config Config, err error) {
	err = mapstructure.Decode(configMap, &config)
	if err != nil {
		return
	}

	return
}

func (e *Extractor) validateConfig(config Config) (err error) {
	if config.ProjectID == "" {
		return errors.New("project_id is required")
	}

	return
}
