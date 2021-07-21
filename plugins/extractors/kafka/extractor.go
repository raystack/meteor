package kafka

import (
	"errors"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/proto/odpf/meta"
)

type Config struct {
	Broker string `json:"broker"`
}

type Extractor struct{}

func New() extractor.TopicExtractor {
	return &Extractor{}
}

func (e *Extractor) Extract(c map[string]interface{}) (result []meta.Topic, err error) {
	// build config
	config, err := e.getConfig(c)
	if err != nil {
		return
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
