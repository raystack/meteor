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

	"github.com/raystack/meteor/models"
	v1beta2 "github.com/raystack/meteor/models/raystack/assets/v1beta2"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/log"
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
	Broker string     `json:"broker" yaml:"broker" mapstructure:"broker" validate:"required"`
	Auth   AuthConfig `json:"auth_config" yaml:"auth_config" mapstructure:"auth_config"`
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
	conn       sarama.Consumer
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

		if e.config.Auth.SASL.Enabled {
			consumerConfig.Net.SASL.Enable = true
			if e.config.Auth.SASL.Mechanism == sarama.SASLTypeOAuth {
				consumerConfig.Net.SASL.Mechanism = sarama.SASLTypeOAuth
				consumerConfig.Net.SASL.TokenProvider = NewKubernetesTokenProvider()
			}
	}

	consumer, err := sarama.NewConsumer([]string{e.config.Broker}, consumerConfig)
	if err != nil {
		fmt.Printf("Error is here !! %s", err.Error())
		return fmt.Errorf("failed to create kafka consumer for brokers %s and config %+v. Error %s", e.config.Broker,
			consumerConfig, err.Error())
	}
	e.conn = consumer
	return nil
}

// Extract checks if the extractor is ready to extract
// if so, then extracts metadata from the kafka broker
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	defer e.conn.Close()

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
	topics, err := e.conn.Topics()
	if err != nil {
		return fmt.Errorf("fetch topics: %w", err)
	}

	// build and push topics
	for _, topic := range topics {
		// skip if topic is a default topic
		_, isDefaultTopic := defaultTopics[topic]
		if isDefaultTopic {
			continue
		}

		partitions, err := e.conn.Partitions(topic)
		if err != nil {
			e.logger.Error("failed to fetch partitions for topic", "err", err, "topic", topic)
			continue
		}
		asset, err := e.buildAsset(topic, len(partitions))
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

	if authConfig.CAFile == "" {
		//nolint:gosec
		return &tls.Config{
			InsecureSkipVerify: e.config.Auth.TLS.InsecureSkipVerify,
		}, nil
	}

	cert, err := tls.LoadX509KeyPair(authConfig.CertFile, authConfig.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("create cert: %w", err)
	}

	var cert tls.Certificate
	var err error
	if authConfig.CertFile != "" && authConfig.KeyFile != "" {
		cert, err = tls.LoadX509KeyPair(authConfig.CertFile, authConfig.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("create cert: %w", err)
		}
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
		Attributes: &structpb.Struct{},
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

func init() {
	if err := registry.Extractors.Register("kafka", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
