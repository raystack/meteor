package kafka

import (
	"context"
	_ "embed" // used to print the embedded assets

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	kafka "github.com/segmentio/kafka-go"

	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

// default topics map to skip
var defaultTopics = map[string]byte{
	"__consumer_offsets": 0,
	"_schemas":           0,
}

// Config holds the set of configuration for the kafka extractor
type Config struct {
	Broker string `mapstructure:"broker" validate:"required"`
}

var sampleConfig = `
broker: "localhost:9092"`

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
	conn   *kafka.Conn
	logger log.Logger
	config Config
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
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// create connection
	e.conn, err = kafka.Dial("tcp", e.config.Broker)
	if err != nil {
		return errors.Wrap(err, "failed to create connection")
	}

	return
}

// Extract checks if the extractor is ready to extract
// if so, then extracts metadata from the kafka broker
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	defer e.conn.Close()

	partitions, err := e.conn.ReadPartitions()
	if err != nil {
		return errors.Wrap(err, "failed to fetch partitions")
	}

	// collect topic list from partition list
	topics := map[string]int{}
	for _, p := range partitions {
		_, ok := topics[p.Topic]
		if !ok {
			topics[p.Topic] = 0
		}

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
		record := models.NewRecord(asset)
		emit(record)
	}

	return
}

// Build topic metadata model using a topic and number of partitions
func (e *Extractor) buildAsset(topicName string, numOfPartitions int) (asset *v1beta2.Asset, err error) {
	topic, err := anypb.New(&v1beta2.Topic{
		Profile: &v1beta2.TopicProfile{
			NumberOfPartitions: int64(numOfPartitions),
		},
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
