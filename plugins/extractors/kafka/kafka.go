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

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
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
	Broker string     `mapstructure:"broker" validate:"required"`
	Auth   AuthConfig `mapstructure:"auth_config"`
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
}

var sampleConfig = `broker: "localhost:9092"`

var info = plugins.Info{
	Description:  "Topic list from Apache Kafka.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the extraction of data
// from a kafka broker
type Extractor struct {
	plugins.BaseExtractor
	// internal states
	conn       *kafka.Conn
	logger     log.Logger
	config     Config
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
	clientDurn, err := otel.Meter("").
		Int64Histogram("meteor.kafka.client.duration", metric.WithUnit("ms"))
	if err != nil {
		return fmt.Errorf("create client duration histogram: %w", err)
	}

	e.clientDurn = clientDurn

	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// create default dialer
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	if e.config.Auth.TLS.Enabled {
		tlsConfig, err := e.createTLSConfig()
		if err != nil {
			return fmt.Errorf("create tls config: %w", err)
		}

		dialer.TLS = tlsConfig
	}

	// create connection
	e.conn, err = dialer.DialContext(ctx, "tcp", e.config.Broker)
	if err != nil {
		return fmt.Errorf("create connection: %w", err)
	}

	return nil
}

// Extract checks if the extractor is ready to extract
// if so, then extracts metadata from the kafka broker
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	defer e.conn.Close()

	partitions, err := e.readPartitions(ctx)
	if err != nil {
		return fmt.Errorf("fetch partitions: %w", err)
	}

	// collect topic list from partition list
	topics := map[string]int{}
	for _, p := range partitions {
		topics[p.Topic]++
	}

	// build and push topics
	for topic, numOfPartitions := range topics {
		// skip if topic is a default topic
		_, isDefaultTopic := defaultTopics[topic]
		if isDefaultTopic {
			continue
		}

		asset, err := e.buildAsset(topic, numOfPartitions)
		if err != nil {
			e.logger.Error("failed to build asset", "err", err, "topic", topic)
			continue
		}
		emit(models.NewRecord(asset))
	}

	return nil
}

func (e *Extractor) createTLSConfig() (*tls.Config, error) {
	authConfig := e.config.Auth.TLS

	if authConfig.CertFile == "" || authConfig.KeyFile == "" || authConfig.CAFile == "" {
		//nolint:gosec
		return &tls.Config{
			InsecureSkipVerify: e.config.Auth.TLS.InsecureSkipVerify,
		}, nil
	}

	cert, err := tls.LoadX509KeyPair(authConfig.CertFile, authConfig.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("create cert: %w", err)
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

// Build topic metadata model using a topic and number of partitions
func (e *Extractor) buildAsset(topicName string, numOfPartitions int) (*v1beta2.Asset, error) {
	topic, err := anypb.New(&v1beta2.Topic{
		Profile: &v1beta2.TopicProfile{
			NumberOfPartitions: int64(numOfPartitions),
		},
		Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
	})
	if err != nil {
		e.logger.Warn("error creating Any struct", "error", err)
	}

	return &v1beta2.Asset{
		Urn:     models.NewURN("kafka", e.UrnScope, "topic", topicName),
		Name:    topicName,
		Service: "kafka",
		Type:    "topic",
		Data:    topic,
	}, nil
}

func (e *Extractor) readPartitions(ctx context.Context) (partitions []kafka.Partition, err error) {
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

	return e.conn.ReadPartitions()
}

func init() {
	if err := registry.Extractors.Register("kafka", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
