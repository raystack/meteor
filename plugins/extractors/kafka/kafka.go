package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	_ "embed" // used to print the embedded assets
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

//go:embed README.md
var summary string

// default topics map to skip
var defaultTopics = map[string]struct{}{
	"__consumer_offsets": {},
	"_schemas":           {},
}

// Config holds the set of configuration for the kafka extractor
type Config struct {
	Broker  string     `json:"broker" yaml:"broker" mapstructure:"broker" validate:"required"`
	Auth    AuthConfig `json:"auth_config" yaml:"auth_config" mapstructure:"auth_config"`
	Extract []string   `json:"extract" yaml:"extract" mapstructure:"extract"`
}

type AuthConfig struct {
	TLS struct {
		// Whether to use TLS when connecting to the broker
		// (defaults to false).
		Enabled bool `mapstructure:"enabled"`

		// controls whether a client verifies the server's certificate chain and host name
		// defaults to false
		InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`

		// certificate file for client authentication
		CertFile string `mapstructure:"cert_file"`

		// key file for client authentication
		KeyFile string `mapstructure:"key_file"`

		// certificate authority file for TLS client authentication
		CAFile string `mapstructure:"ca_file"`
	} `mapstructure:"tls"`

	SASL struct {
		Enabled   bool   `mapstructure:"enabled"`
		Mechanism string `mapstructure:"mechanism"`
	}
}

var sampleConfig = `
broker: "localhost:9092"
# extract specifies which entity types to extract.
# Defaults to all: ["topics", "consumer_groups"]
extract:
  - topics
  - consumer_groups`

var info = plugins.Info{
	Description:  "Topic and consumer group metadata from Apache Kafka.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "streaming"},
	Entities: []plugins.EntityInfo{
		{Type: "topic", URNPattern: "urn:kafka:{scope}:topic:{topic_name}"},
		{Type: "consumer_group", URNPattern: "urn:kafka:{scope}:consumer_group:{group_id}"},
	},
	Edges: []plugins.EdgeInfo{
		{Type: "consumed_by", From: "consumer_group", To: "topic"},
	},
}

// Extractor manages the extraction of data
// from a kafka broker
type Extractor struct {
	plugins.BaseExtractor
	// internal states
	conn       sarama.Consumer
	admin      sarama.ClusterAdmin
	logger     log.Logger
	config     Config
	extract    map[string]bool
	clientDurn metric.Int64Histogram
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	clientDurn, err := otel.Meter("github.com/raystack/meteor/plugins/extractors/kafka").
		Int64Histogram("meteor.kafka.client.duration", metric.WithUnit("ms"))
	if err != nil {
		otel.Handle(err)
	}

	e.clientDurn = clientDurn

	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	consumerConfig := sarama.NewConfig()
	if e.config.Auth.TLS.Enabled {
		tlsConfig, err := e.createTLSConfig()
		if err != nil {
			return fmt.Errorf("create tls config: %w", err)
		}
		consumerConfig.Net.TLS.Enable = true
		consumerConfig.Net.TLS.Config = tlsConfig
	}

	if e.config.Auth.SASL.Enabled {
		consumerConfig.Net.SASL.Enable = true
		if e.config.Auth.SASL.Mechanism == sarama.SASLTypeOAuth {
			consumerConfig.Net.SASL.Mechanism = sarama.SASLTypeOAuth
			consumerConfig.Net.SASL.TokenProvider = NewKubernetesTokenProvider()
		}
	}

	consumer, err := sarama.NewConsumer([]string{e.config.Broker}, consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create kafka consumer for brokers %s and config %+v. Error %s", e.config.Broker,
			consumerConfig, err.Error())
	}
	e.conn = consumer

	admin, err := sarama.NewClusterAdmin([]string{e.config.Broker}, consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create kafka cluster admin: %w", err)
	}
	e.admin = admin

	e.extract = map[string]bool{
		"topics":          true,
		"consumer_groups": true,
	}
	if len(e.config.Extract) > 0 {
		e.extract = make(map[string]bool, len(e.config.Extract))
		for _, v := range e.config.Extract {
			e.extract[v] = true
		}
	}

	return nil
}

// Extract checks if the extractor is ready to extract
// if so, then extracts metadata from the kafka broker
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	defer e.conn.Close()
	defer e.admin.Close()

	defer func(start time.Time) {
		attributes := []attribute.KeyValue{
			attribute.String("kafka.broker", e.config.Broker),
			attribute.Bool("success", err == nil),
		}
		if err != nil {
			errorCode := "UNKNOWN"
			var kErr kafka.Error
			if errors.As(err, &kErr) {
				errorCode = strings.ReplaceAll(
					strings.ToUpper(kErr.Title()), " ", "_",
				)
			}
			attributes = append(attributes, attribute.String("kafka.error_code", errorCode))
		}

		e.clientDurn.Record(
			ctx, time.Since(start).Milliseconds(), metric.WithAttributes(attributes...),
		)
	}(time.Now())

	if e.extract["topics"] {
		if err := e.extractTopics(ctx, emit); err != nil {
			return fmt.Errorf("extract topics: %w", err)
		}
	}

	if e.extract["consumer_groups"] {
		if err := e.extractConsumerGroups(ctx, emit); err != nil {
			return fmt.Errorf("extract consumer_groups: %w", err)
		}
	}

	return nil
}

func (e *Extractor) extractTopics(_ context.Context, emit plugins.Emit) error {
	topics, err := e.conn.Topics()
	if err != nil {
		return fmt.Errorf("fetch topics: %w", err)
	}

	// Fetch topic metadata for replication factor.
	topicMetadata, err := e.admin.DescribeTopics(topics)
	if err != nil {
		e.logger.Warn("failed to describe topics, continuing with basic metadata", "err", err)
		topicMetadata = nil
	}

	metadataByName := make(map[string]*sarama.TopicMetadata, len(topicMetadata))
	for _, tm := range topicMetadata {
		metadataByName[tm.Name] = tm
	}

	for _, topic := range topics {
		if _, isDefault := defaultTopics[topic]; isDefault {
			continue
		}

		partitions, err := e.conn.Partitions(topic)
		if err != nil {
			e.logger.Error("failed to fetch partitions for topic", "err", err, "topic", topic)
			continue
		}

		record := e.buildTopicRecord(topic, len(partitions), metadataByName[topic])
		emit(record)
	}
	return nil
}

func (e *Extractor) extractConsumerGroups(_ context.Context, emit plugins.Emit) error {
	groups, err := e.admin.ListConsumerGroups()
	if err != nil {
		return fmt.Errorf("list consumer groups: %w", err)
	}

	groupNames := make([]string, 0, len(groups))
	for name := range groups {
		groupNames = append(groupNames, name)
	}

	if len(groupNames) == 0 {
		return nil
	}

	descriptions, err := e.admin.DescribeConsumerGroups(groupNames)
	if err != nil {
		return fmt.Errorf("describe consumer groups: %w", err)
	}

	for _, desc := range descriptions {
		record := e.buildConsumerGroupRecord(desc)
		emit(record)
	}
	return nil
}

func (e *Extractor) buildTopicRecord(topicName string, numOfPartitions int, metadata *sarama.TopicMetadata) models.Record {
	props := map[string]any{
		"number_of_partitions": int64(numOfPartitions),
	}

	if metadata != nil && len(metadata.Partitions) > 0 {
		props["replication_factor"] = int64(len(metadata.Partitions[0].Replicas))
	}

	// Fetch topic config entries (retention.ms, cleanup.policy, etc.).
	configEntries, err := e.admin.DescribeConfig(sarama.ConfigResource{
		Type: sarama.TopicResource,
		Name: topicName,
	})
	if err == nil {
		for _, entry := range configEntries {
			switch entry.Name {
			case "retention.ms":
				props["retention_ms"] = entry.Value
			case "cleanup.policy":
				props["cleanup_policy"] = entry.Value
			case "min.insync.replicas":
				props["min_insync_replicas"] = entry.Value
			}
		}
	}

	entity := models.NewEntity(
		models.NewURN("kafka", e.UrnScope, "topic", topicName),
		"topic",
		topicName,
		"kafka",
		props,
	)
	return models.NewRecord(entity)
}

func (e *Extractor) buildConsumerGroupRecord(desc *sarama.GroupDescription) models.Record {
	urn := models.NewURN("kafka", e.UrnScope, "consumer_group", desc.GroupId)

	// Collect unique topics consumed by this group from member assignments.
	consumedTopics := make(map[string]struct{})
	members := make([]any, 0, len(desc.Members))
	for memberID, member := range desc.Members {
		memberProps := map[string]any{
			"member_id": memberID,
			"client_id": member.ClientId,
			"host":      member.ClientHost,
		}
		members = append(members, memberProps)

		assignment, err := member.GetMemberAssignment()
		if err == nil && assignment != nil {
			for topic := range assignment.Topics {
				consumedTopics[topic] = struct{}{}
			}
		}
	}

	props := map[string]any{
		"state":         desc.State,
		"protocol":      desc.Protocol,
		"protocol_type": desc.ProtocolType,
		"num_members":   int64(len(desc.Members)),
	}
	if len(members) > 0 {
		props["members"] = members
	}

	entity := models.NewEntity(urn, "consumer_group", desc.GroupId, "kafka", props)

	// Create edges from consumer group to each consumed topic.
	var edges []*meteorv1beta1.Edge
	for topic := range consumedTopics {
		edges = append(edges, &meteorv1beta1.Edge{
			SourceUrn: urn,
			TargetUrn: models.NewURN("kafka", e.UrnScope, "topic", topic),
			Type:      "consumed_by",
			Source:    "kafka",
		})
	}

	return models.NewRecord(entity, edges...)
}

func (e *Extractor) createTLSConfig() (*tls.Config, error) {
	authConfig := e.config.Auth.TLS

	if authConfig.CAFile == "" {
		//nolint:gosec
		return &tls.Config{
			InsecureSkipVerify: e.config.Auth.TLS.InsecureSkipVerify,
		}, nil
	}

	var cert tls.Certificate
	var err error
	if authConfig.CertFile != "" && authConfig.KeyFile != "" {
		cert, err = tls.LoadX509KeyPair(authConfig.CertFile, authConfig.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("create cert: %w", err)
		}
	}

	caCert, err := os.ReadFile(authConfig.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read ca cert file: %w", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	//nolint:gosec
	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: e.config.Auth.TLS.InsecureSkipVerify,
	}, nil
}

func init() {
	if err := registry.Extractors.Register("kafka", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
