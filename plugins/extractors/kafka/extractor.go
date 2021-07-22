package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/utils"
)

type Config struct {
	Broker string `mapstructure:"broker" validate:"required"`
}

type Extractor struct {
	logger plugins.Logger
}

func New(logger plugins.Logger) extractor.TopicExtractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []meta.Topic, err error) {
	e.logger.Info("extracting kafka metadata...")
	// build config
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return result, extractor.InvalidConfigError{}
	}

	// create client
	client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"metadata.broker.list": config.Broker,
	})
	if err != nil {
		return result, err
	}
	defer client.Close()

	// fetch and build metadata
	metadata, err := client.GetMetadata(nil, true, 1000)
	if err != nil {
		return result, err
	}
	for topic := range metadata.Topics {
		result = append(result, meta.Topic{
			Urn:    topic,
			Name:   topic,
			Source: "kafka",
		})
	}

	return result, err
}
