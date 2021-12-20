package kafka

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/pkg/errors"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	kafka "github.com/segmentio/kafka-go"

	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

// default topics map to skip
var defaultTopics = map[string]byte{
	"__consumer_offsets": 0,
	"_schemas":           0,
}

// Config hold the set of configuration for the kafka extractor
type Config struct {
	Broker string `mapstructure:"broker" validate:"required"`
	Label  string `mapstructure:"label" validate:"required"`
}

var sampleConfig = `
broker: "localhost:9092"
label: "my-kafka"`

// Extractor manages the extraction of data
// from a kafka broker
type Extractor struct {
	// internal states
	conn   *kafka.Conn
	logger log.Logger
	config Config
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Topic list from Apache Kafka.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
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

		record := models.NewRecord(e.buildTopic(topic, numOfPartitions))
		emit(record)
	}

	return
}

// Build topic metadata model using a topic and number of partitions
func (e *Extractor) buildTopic(topic string, numOfPartitions int) *assetsv1beta1.Topic {
	return &assetsv1beta1.Topic{
		Resource: &commonv1beta1.Resource{
			Urn:     fmt.Sprintf("kafka::%s/%s", e.config.Label, topic),
			Name:    topic,
			Service: "kafka",
		},
		Profile: &assetsv1beta1.TopicProfile{
			NumberOfPartitions: int64(numOfPartitions),
		},
	}
}

func init() {
	if err := registry.Extractors.Register("kafka", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
