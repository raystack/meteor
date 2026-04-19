package gcs

import (
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/base64"
	"fmt"

	"errors"

	"cloud.google.com/go/storage"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sqlutil"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the extractor
type Config struct {
	ProjectID string `json:"project_id" yaml:"project_id" mapstructure:"project_id" validate:"required"`
	// ServiceAccountBase64 takes precedence over ServiceAccountJSON field
	ServiceAccountBase64 string   `json:"service_account_base64" yaml:"service_account_base64" mapstructure:"service_account_base64"`
	ServiceAccountJSON   string   `json:"service_account_json" yaml:"service_account_json" mapstructure:"service_account_json"`
	ExtractBlob          bool     `json:"extract_blob" yaml:"extract_blob" mapstructure:"extract_blob"`
	Exclude              []string `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

var sampleConfig = `
project_id: google-project-id
extract_blob: true
# Only one of service_account_base64 / service_account_json is needed.
# If both are present, service_account_base64 takes precedence
service_account_base64: ____base64_encoded_service_account____
service_account_json: |-
  {
    "type": "service_account",
    "private_key_id": "xxxxxxx",
    "private_key": "xxxxxxx",
    "client_email": "xxxxxxx",
    "client_id": "xxxxxxx",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "xxxxxxx",
    "client_x509_cert_url": "xxxxxxx"
  }
exclude: [bucket_a,bucket_b]`

var info = plugins.Info{
	Description:  "Bucket and blob metadata from Google Cloud Storage.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"gcp", "storage"},
	Entities: []plugins.EntityInfo{
		{Type: "bucket", URNPattern: "urn:gcs:{project_id}:bucket:{bucket_name}"},
	},
}

// Extractor manages the extraction of data
// from the google cloud storage
type Extractor struct {
	plugins.BaseExtractor
	client          *storage.Client
	logger          log.Logger
	config          Config
	excludedBuckets map[string]bool
	newClient       NewClientFunc
}

type NewClientFunc func(ctx context.Context, logger log.Logger, config Config) (*storage.Client, error)

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger, newClient NewClientFunc) *Extractor {
	e := &Extractor{
		logger:    logger,
		newClient: newClient,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)
	e.ScopeNotRequired = true

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// build excluded buckets map
	e.excludedBuckets = sqlutil.BuildBoolMap(e.config.Exclude)

	// create client
	var err error
	e.client, err = e.newClient(ctx, e.logger, e.config)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}

	return nil
}

func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	it := e.client.Buckets(ctx, e.config.ProjectID)
	for {
		bucket, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("iterate over %s: %w", bucket.Name, err)
		}

		// skip excluded buckets
		if e.excludedBuckets[bucket.Name] {
			continue
		}

		var blobs []map[string]any
		if e.config.ExtractBlob {
			blobs, err = e.extractBlobs(ctx, bucket.Name, e.config.ProjectID)
			if err != nil {
				return fmt.Errorf("extract blobs from %s: %w", bucket.Name, err)
			}
		}
		record := e.buildBucket(bucket, e.config.ProjectID, blobs)
		emit(record)
	}

	return
}

func (e *Extractor) extractBlobs(ctx context.Context, bucketName, projectID string) ([]map[string]any, error) {
	it := e.client.Bucket(bucketName).Objects(ctx, nil)

	var blobs []map[string]any
	for {
		object, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return blobs, nil
		}
		if err != nil {
			return nil, err
		}

		blobs = append(blobs, e.buildBlob(object, projectID))
	}
}

func (e *Extractor) buildBucket(b *storage.BucketAttrs, projectID string, blobs []map[string]any) models.Record {
	urn := models.NewURN("gcs", projectID, "bucket", b.Name)

	props := map[string]any{
		"location":           b.Location,
		"storage_type":       b.StorageClass,
		"versioning_enabled": b.VersioningEnabled,
		"requester_pays":     b.RequesterPays,
	}
	if !b.Created.IsZero() {
		props["create_time"] = b.Created.Format("2006-01-02T15:04:05Z")
	}
	if len(blobs) > 0 {
		props["blobs"] = blobs
	}
	if len(b.Labels) > 0 {
		props["labels"] = b.Labels
	}
	if b.RetentionPolicy != nil {
		props["retention_period_seconds"] = int64(b.RetentionPolicy.RetentionPeriod.Seconds())
	}
	if b.Encryption != nil && b.Encryption.DefaultKMSKeyName != "" {
		props["default_kms_key"] = b.Encryption.DefaultKMSKeyName
	}
	if b.Logging != nil && b.Logging.LogBucket != "" {
		props["log_bucket"] = b.Logging.LogBucket
	}
	if b.LocationType != "" {
		props["location_type"] = b.LocationType
	}

	entity := models.NewEntity(urn, "bucket", b.Name, "gcs", props)
	return models.NewRecord(entity)
}

func (e *Extractor) buildBlob(blob *storage.ObjectAttrs, projectID string) map[string]any {
	b := map[string]any{
		"urn":  models.NewURN("gcs", projectID, "object", fmt.Sprintf("%s/%s", blob.Bucket, blob.Name)),
		"name": blob.Name,
		"size": blob.Size,
	}
	if !blob.Deleted.IsZero() {
		b["delete_time"] = blob.Deleted.Format("2006-01-02T15:04:05Z")
	}
	if !blob.RetentionExpirationTime.IsZero() {
		b["expire_time"] = blob.RetentionExpirationTime.Format("2006-01-02T15:04:05Z")
	}
	if blob.Owner != "" {
		b["owner"] = blob.Owner
	}
	if !blob.Created.IsZero() {
		b["create_time"] = blob.Created.Format("2006-01-02T15:04:05Z")
	}
	if !blob.Updated.IsZero() {
		b["update_time"] = blob.Updated.Format("2006-01-02T15:04:05Z")
	}
	return b
}

func createClient(ctx context.Context, logger log.Logger, config Config) (*storage.Client, error) {
	if config.ServiceAccountBase64 == "" && config.ServiceAccountJSON == "" {
		logger.Info("credentials are not specified, creating google cloud storage client using Default Credentials...")
		return storage.NewClient(ctx)
	}

	if config.ServiceAccountBase64 != "" {
		serviceAccountJSON, err := base64.StdEncoding.DecodeString(config.ServiceAccountBase64)
		if err != nil || len(serviceAccountJSON) == 0 {
			return nil, fmt.Errorf("decode Base64 encoded service account: %w", err)
		}
		// overwrite ServiceAccountJSON with credentials from ServiceAccountBase64 value
		config.ServiceAccountJSON = string(serviceAccountJSON)
	}

	return storage.NewClient(ctx, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(config.ServiceAccountJSON)))
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("gcs", func() plugins.Extractor {
		return New(plugins.GetLog(), createClient)
	}); err != nil {
		panic(err)
	}
}
