package kafka

import (
	"errors"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins/utils"
	"github.com/odpf/meteor/proto/odpf/meta"
)

type Config struct {
	Broker string `mapstructure:"broker" validate:"required"`
}

type Extractor struct{}

func New() extractor.TopicExtractor {
	return &Extractor{}
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []meta.Topic, err error) {
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

func (e *Extractor) getConfig(configMap map[string]interface{}) (config Config, err error) {
	err = mapstructure.Decode(configMap, &config)
	if err != nil {
		return
	}
	err = e.validateConfig(config)
	if err != nil {
		return config, extractor.InvalidConfigError{}
	}

	return
}

func (e *Extractor) validateConfig(config Config) error {
	if config.Broker == "" {
		return errors.New("broker is required")
	}

	return nil
}
