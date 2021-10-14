package kafka

import (
	"context"
	_ "embed" // used to print the embedded assets

	"github.com/pkg/errors"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	kafka "github.com/segmentio/kafka-go"

	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

// Config hold the set of configuration for the kafka extractor
type Config struct {
	Broker    string `mapstructure:"broker" validate:"required"`
	UrnPrefix string `mapstructure:"urn_prefix"`
}

var sampleConfig = `
broker: "localhost:9092"`

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
		record := models.NewRecord(e.buildTopic(topic, numOfPartitions))
		emit(record)
	}

	return
}

// Build topic metadata model using a topic and number of partitions
func (e *Extractor) buildTopic(topic string, numOfPartitions int) *assets.Topic {
	return &assets.Topic{
		Resource: &common.Resource{
			Urn:     e.buildUrn(topic),
			Name:    topic,
			Service: "kafka",
		},
		Profile: &assets.TopicProfile{
			NumberOfPartitions: int64(numOfPartitions),
		},
	}
}

// Build urn using prefixes from config
func (e *Extractor) buildUrn(topic string) string {
	if e.config.UrnPrefix != "" {
		topic = e.config.UrnPrefix + topic
	}

	return topic
}

func init() {
	if err := registry.Extractors.Register("kafka", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
