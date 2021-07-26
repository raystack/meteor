package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/core/sink"
)

type Sink struct{}

func init() {
	if err := sink.Catalog.Register("kafka", func() core.Syncer {
		return &Sink{}
	}); err != nil {
		panic(err)
	}
}

// Kafka sink
func (s *Sink) Sink(ctx context.Context, config map[string]interface{}, out <-chan interface{}) (err error) {

	producer, err := getProducer(config)
	if err != nil {
		return err
	}
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)
	for val := range out {
		if err := s.Push(producer, deliveryChan, val, config); err != nil {
			return err
		}
	}
	producer.Flush(5000)
	return nil
}

func getProducer(config map[string]interface{}) (*kafka.Producer, error) {
	if config["brokers"] == nil {
		return nil, errors.New("missing required configs: 'brokers'")
	}
	if config["topic"] == nil {
		return nil, errors.New("missing required configs: 'topic'")
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafka.ConfigValue(config["brokers"].(string)),
		"acks":              "all"})

	if err != nil {
		err = fmt.Errorf("failed to create kafka producer: %w", err)
		return nil, err
	}
	return p, nil
}

func (s *Sink) Push(producer *kafka.Producer, deliveryChan chan kafka.Event, value interface{}, config map[string]interface{}) error {
	data, err := json.Marshal(value)

	if err != nil {
		return errors.New("payload is not a protobuf")
	}

	topic, _ := config["topic"].(string)

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          data,
	}, nil)
	if err != nil {
		return err
	}
	return nil
}
