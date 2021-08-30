package gcs

import (
	"context"
	"fmt"

	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/proto/odpf/assets/facets"
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

var (
	configInfo = ``
	inputInfo  = `
Input:
________________________________________________________________________________________________________________
| Key               | Example                             | Description                            |            |
|___________________|_____________________________________|________________________________________|____________|
| "project_id"      | "my-project"                        | BigQuery Project ID                    | *required* |
| "credentials_json"| "{'private_key':., 'private_id':.}" | Service Account in JSON string         | *optional* |
| "extract_blob"    | "true"                              | Extract blob metadata inside a bucket  | *optional* |
|___________________|_____________________________________|________________________________________|____________|
`
	outputInfo = `
Output:
 _______________________________________________________
|Field                         |Sample Value            |
|______________________________|________________________|
|"resource.urn"                |"project_id/bucket_name"|
|"resource.name"               |"my_table"              |
|"resource.service"            |"googlecloudstorage"    |
|"location"                    |"ASIA"                  |
|"storage_type"                |"STANDARD"              |
|"timestamp.created_at.seconds"|"1551082913"            |
|"timestamp.created_at.nanos"  |"1551082913"            |
|"labels"                      |[]{key:value}           |
|______________________________|________________________|`
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

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) GetDescription() string {
	return inputInfo + outputInfo
}

func (e *Extractor) GetSampleConfig() string {
	return configInfo
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

		var blobs []*assets.Blob
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

func (e *Extractor) extractBlobs(ctx context.Context, bucketName string, projectID string) (blobs []*assets.Blob, err error) {
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

func (e *Extractor) buildBucket(b *storage.BucketAttrs, projectID string, blobs []*assets.Blob) (bucket assets.Bucket) {
	bucket = assets.Bucket{
		Resource: &common.Resource{
			Urn:     fmt.Sprintf("%s/%s", projectID, b.Name),
			Name:    b.Name,
			Service: metadataSource,
		},
		Location:    b.Location,
		StorageType: b.StorageClass,
		Timestamps: &common.Timestamp{
			CreateTime: timestamppb.New(b.Created),
		},
		Properties: &facets.Properties{
			Labels: b.Labels,
		},
	}
	if blobs != nil {
		bucket.Blobs = blobs
	}

	return
}

func (e *Extractor) buildBlob(blob *storage.ObjectAttrs, projectID string) *assets.Blob {
	return &assets.Blob{
		Urn:        fmt.Sprintf("%s/%s/%s", projectID, blob.Bucket, blob.Name),
		Name:       blob.Name,
		Size:       blob.Size,
		DeleteTime: timestamppb.New(blob.Deleted),
		ExpireTime: timestamppb.New(blob.RetentionExpirationTime),
		Ownership: &facets.Ownership{
			Owners: []*facets.Owner{
				{Name: blob.Owner},
			},
		},
		Timestamps: &common.Timestamp{
			CreateTime: timestamppb.New(blob.Created),
			UpdateTime: timestamppb.New(blob.Updated),
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

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("gcs", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
