package gcs

import (
	"context"
	"fmt"

	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/common"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/odpf/meteor/registry"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cloud.google.com/go/storage"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	metadataSource = "googlecloudstorage"
)

type Config struct {
	ProjectID          string `mapstructure:"project_id" validate:"required"`
	ServiceAccountJSON string `mapstructure:"service_account_json"`
	ExtractBlob        bool   `mapstructure:"extract_blob"`
}

type Extractor struct {
	client *storage.Client

	// dependencies
	logger log.Logger
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	// build config
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	// create client
	client, err := e.createClient(ctx, config)
	if err != nil {
		return
	}
	e.client = client

	return e.extract(ctx, out, config)
}

func (e *Extractor) extract(ctx context.Context, out chan<- interface{}, config Config) (err error) {
	it := e.client.Buckets(ctx, config.ProjectID)
	for {
		bucket, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		var blobs []*facets.Blob
		if config.ExtractBlob {
			blobs, err = e.extractBlobs(ctx, bucket.Name, config.ProjectID)
			if err != nil {
				return err
			}
		}

		out <- e.buildBucket(bucket, config.ProjectID, blobs)
	}

	return
}

func (e *Extractor) extractBlobs(ctx context.Context, bucketName string, projectID string) (blobs []*facets.Blob, err error) {
	it := e.client.Bucket(bucketName).Objects(ctx, nil)

	object, err := it.Next()
	for err == nil {
		blobs = append(blobs, e.buildBlob(object, projectID))
		object, err = it.Next()
	}
	if err == iterator.Done {
		err = nil
	}

	return
}

func (e *Extractor) buildBucket(b *storage.BucketAttrs, projectID string, blobs []*facets.Blob) (bucket meta.Bucket) {
	bucket = meta.Bucket{
		Urn:         fmt.Sprintf("%s/%s", projectID, b.Name),
		Name:        b.Name,
		Location:    b.Location,
		StorageType: b.StorageClass,
		Source:      metadataSource,
		Timestamps: &common.Timestamp{
			CreatedAt: timestamppb.New(b.Created),
		},
		Tags: &facets.Tags{
			Tags: b.Labels,
		},
	}
	if blobs != nil {
		bucket.Blobs = &facets.Blobs{
			Blobs: blobs,
		}
	}

	return
}

func (e *Extractor) buildBlob(blob *storage.ObjectAttrs, projectID string) *facets.Blob {
	return &facets.Blob{
		Urn:       fmt.Sprintf("%s/%s/%s", projectID, blob.Bucket, blob.Name),
		Name:      blob.Name,
		Size:      blob.Size,
		DeletedAt: timestamppb.New(blob.Deleted),
		ExpiredAt: timestamppb.New(blob.RetentionExpirationTime),
		Ownership: &facets.Ownership{
			Owners: []*facets.Owner{
				{Name: blob.Owner},
			},
		},
		Timestamps: &common.Timestamp{
			CreatedAt: timestamppb.New(blob.Created),
			UpdatedAt: timestamppb.New(blob.Updated),
		},
	}
}

func (e *Extractor) createClient(ctx context.Context, config Config) (*storage.Client, error) {
	if config.ServiceAccountJSON == "" {
		e.logger.Info("credentials are not specified, creating google cloud storage client using Default Credentials...")
		return storage.NewClient(ctx)
	}

	return storage.NewClient(ctx, option.WithCredentialsJSON([]byte(config.ServiceAccountJSON)))
}

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("gcs", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
