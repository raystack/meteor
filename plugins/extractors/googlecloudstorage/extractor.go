package googlecloudstorage

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/common"
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

type Bucket struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
	Urn          string
	BucketName   string
	Location     string
	LocationType string
	StorageClass string
	Timestamps   *common.Timestamp
	Tags         *facets.Tags
	Event        *common.Event
	Blobs        []Blob
}
type Blob struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
	Urn         string               `protobuf:"bytes,1,opt,name=urn,proto3" json:"urn,omitempty"`
	Name        string               `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Source      string               `protobuf:"bytes,3,opt,name=source,proto3" json:"source,omitempty"`
	Description string               `protobuf:"bytes,4,opt,name=description,proto3" json:"description,omitempty"`
	BucketUrn   string               `protobuf:"bytes,5,opt,name=bucket_urn,proto3" json:"bucket_urn,omitempty"`
	Size        Integer              `protobuf:"bytes,6,opt,name=size,proto3" json:"size,omitempty"`
	DeletedAt   *timestamp.Timestamp `protobuf:"bytes,7,opt,name=deleted_at,proto3" json:"deleted_at,omitempty"`
	ExpiredAt   *timestamp.Timestamp `protobuf:"bytes,8,opt,name=expired_at,proto3" json:"expired_at,omitempty"`
	Ownership   *facets.Ownership    `protobuf:"bytes,9,opt,name=ownership,proto3" json:"ownership,omitempty"`
	Tags        *facets.Tags         `protobuf:"bytes,21,opt,name=tags,proto3" json:"tags,omitempty"`
	Custom      *facets.Custom       `protobuf:"bytes,22,opt,name=custom,proto3" json:"custom,omitempty"`
	Timestamps  *common.Timestamp    `protobuf:"bytes,23,opt,name=timestamps,proto3" json:"timestamps,omitempty"`
	Event       *common.Event        `protobuf:"bytes,100,opt,name=event,proto3" json:"event,omitempty"`
}

func New(logger plugins.Logger) extractor.TableExtractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []Bucket, err error) {
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

func (e *Extractor) getMetadata(ctx context.Context, client *storage.Client, projectID string) ([]Bucket, err error) {
	it := client.Buckets(ctx, projectID)
	var results []Bucket

	bucket, err := it.Next()
	for err == nil {
		bucketName := bucket.Name
		results, err = e.appendObjectsMetadata(ctx, bucketName, client)
		if err != nil {
			return
		}

		bucket, err = it.Next()
	}
	if err == iterator.Done {
		err = nil
	}

	return
}

func (e *Extractor) appendObjectsMetadata(ctx context.Context, bucketName string, client *storage.Client) ([]Blob, error) {
	it := client.Bucket(bucketName).Objects(ctx, nil)
	var results []Blob

	object, err := it.Next()
	for err == nil {
		results = append(results, e.mapObject(object))
		object, err = it.Next()
	}
	if err == iterator.Done {
		err = nil
	}

	return results, err
}

func (e *Extractor) mapTable(b *storage.BucketAttrs, projectID string) Bucket {
	return Bucket{
		Urn          : fmt.Sprintf("%s/%s", ProjectID, b.Name),
		BucketName   : b.Name,
		Location     : b.Location,
		LocationType : b.LocationType,
		StorageClass : b.StorageClass,
		Timestamps   : &common.Timestamp{
			CreatedAt: b.Created,
		},
		Tags: b.Labels,
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
