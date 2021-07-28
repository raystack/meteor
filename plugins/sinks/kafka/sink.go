package kafka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/proto"
	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/core/sink"
	"github.com/odpf/meteor/utils"
	"github.com/pkg/errors"
)

type Config struct {
	Brokers string `mapstructure:"brokers" validate:"required"`
	Topic   string `mapstructure:"topic" validate:"required"`
}

type Sink struct{}

func New() core.Syncer {
	return new(Sink)
}

func (s *Sink) Sink(ctx context.Context, config map[string]interface{}, in <-chan interface{}) (err error) {
	kafkaConf := &Config{}
	if err := utils.BuildConfig(config, kafkaConf); err != nil {
		return err
	}
	producer, err := getProducer(kafkaConf.Brokers)
	if err != nil {
		return errors.Wrapf(err, "failed to create kafka producer")
	}
	defer producer.Flush(5000)
	for val := range in {
		if err := s.push(producer, val, kafkaConf.Topic); err != nil {
			return err
		}
	}
	return nil
}

func getProducer(brokers string) (*kafka.Producer, error) {
	producerConf := &kafka.ConfigMap{}
	producerConf.SetKey("bootstrap.servers", brokers)
	producerConf.SetKey("acks", "all")
	return kafka.NewProducer(producerConf)
}

func (s *Sink) push(producer *kafka.Producer, value interface{}, topic string) error {
	protoBytes, err := proto.Marshal(value.(proto.Message))
	if err != nil {
		return errors.Wrap(err, "failed to serialize payload as a protobuf message")
	}

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          protoBytes,
	}, nil)
	if err != nil {
		return err
	}
	return nil
}

func init() {
	if err := sink.Catalog.Register("kafka", func() core.Syncer {
		return &Sink{}
	}); err != nil {
		panic(err)
	}
}
