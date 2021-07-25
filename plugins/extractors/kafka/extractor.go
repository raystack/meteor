package kafka

import (
	"context"

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

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	e.logger.Info("extracting kafka metadata...")
	// build config
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return extractor.InvalidConfigError{}
	}

	// create client
	client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"metadata.broker.list": config.Broker,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	// fetch and build metadata
	metadata, err := client.GetMetadata(nil, true, 1000)
	if err != nil {
		return err
	}
	for topic := range metadata.Topics {
		out <- meta.Topic{
			Urn:    topic,
			Name:   topic,
			Source: "kafka",
		}
	}

	return nil
}

func init() {
	if err := extractor.Catalog.Register("kafka", &Extractor{
		logger: plugins.Log,
	}); err != nil {
		panic(err)
	}
}
