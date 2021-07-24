package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/core/sink"
)

type Sink struct{}

func init() {
	if err := sink.Catalog.Register("kafka", New()); err != nil {
		panic(err)
	}
}

func New() core.Syncer {
	return new(Sink)
}

// TODO
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
	valueData, ok := value.(core.JSONCodec)

	if !ok {
		return errors.New("payload is not a protobuf")
	}
	msgVal, err := valueData.ToJSON()
	if err != nil {
		return err
	}

	topic, _ := config["topic"].(string)

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          msgVal,
	}, nil)
	return nil

}
