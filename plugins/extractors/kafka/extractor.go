package kafka

import (
	"context"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/entities/resources"
	"github.com/odpf/meteor/registry"
	kafka "github.com/segmentio/kafka-go"

	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

type Config struct {
	Broker string `mapstructure:"broker" validate:"required"`
}

type Extractor struct {
	// internal states
	out  chan<- interface{}
	conn *kafka.Conn

	// dependencies
	logger log.Logger
}

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	e.out = out

	// build config
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	// create conn
	e.conn, err = kafka.Dial("tcp", config.Broker)
	if err != nil {
		return err
	}
	defer e.conn.Close()

	return e.extract()
}

// Extract and output metadata from all topics in a broker
func (e *Extractor) extract() (err error) {
	partitions, err := e.conn.ReadPartitions()
	if err != nil {
		return
	}

	// collect topic list from partition list
	topics := map[string]bool{}
	for _, p := range partitions {
		topics[p.Topic] = true
	}

	// process topics
	for topic_name := range topics {
		e.out <- e.buildTopic(topic_name)
	}

	return
}

// Build topic metadata model using a topic name
func (e *Extractor) buildTopic(topic_name string) resources.Topic {
	return resources.Topic{
		Urn:    topic_name,
		Name:   topic_name,
		Source: "kafka",
	}
}

func init() {
	if err := registry.Extractors.Register("kafka", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
