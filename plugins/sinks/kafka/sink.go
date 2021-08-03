package kafka

import (
	"context"
	"reflect"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

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
	KeyPath string `mapstructure:"key_path"`
}

type ProtoReflector interface {
	ProtoReflect() protoreflect.Message
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
		if err := s.push(producer, kafkaConf, val); err != nil {
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

func (s *Sink) push(producer *kafka.Producer, conf *Config, payload interface{}) error {
	kafkaValue, err := s.buildValue(payload)
	if err != nil {
		return err
	}

	kafkaKey, err := s.buildKey(payload, conf.KeyPath)
	if err != nil {
		return err
	}

	return producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &conf.Topic},
		Key:            kafkaKey,
		Value:          kafkaValue,
	}, nil)
}

func (s *Sink) buildValue(value interface{}) ([]byte, error) {
	protoBytes, err := proto.Marshal(value.(proto.Message))
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize payload as a protobuf message")
	}
	return protoBytes, nil
}

// we can optimize this by caching descriptor and key path
func (s *Sink) buildKey(payload interface{}, keyPath string) ([]byte, error) {
	if keyPath == "" {
		return nil, nil
	}

	// extract key field name and value
	fieldName, err := s.getTopLevelKeyFromPath(keyPath)
	if err != nil {
		return nil, err
	}
	keyString, keyJsonName, err := s.extractKeyFromPayload(fieldName, payload)
	if err != nil {
		return nil, err
	}

	// get descriptor
	reflector, ok := payload.(ProtoReflector)
	if !ok {
		return nil, errors.New("not a valid proto payload")
	}
	messageDescriptor := reflector.ProtoReflect().Descriptor()
	fieldDescriptor := messageDescriptor.Fields().ByJSONName(keyJsonName)
	if fieldDescriptor == nil {
		return nil, errors.New("failed to build kafka key")
	}

	// populate message
	dynamicMsgKey := dynamicpb.NewMessage(messageDescriptor)
	dynamicMsgKey.Set(fieldDescriptor, protoreflect.ValueOfString(keyString))
	return proto.Marshal(dynamicMsgKey)
}

func (s *Sink) extractKeyFromPayload(fieldName string, value interface{}) (string, string, error) {
	valueOf := reflect.ValueOf(value)
	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}
	if valueOf.Kind() != reflect.Struct {
		return "", "", errors.New("invalid data")
	}

	structField, ok := valueOf.Type().FieldByName(fieldName)
	if !ok {
		return "", "", errors.New("invalid path, unknown field")
	}
	jsonName := strings.Split(structField.Tag.Get("json"), ",")[0]

	fieldVal := valueOf.FieldByName(fieldName)
	if !fieldVal.IsValid() || fieldVal.IsZero() {
		return "", "", errors.New("invalid path, unknown field")
	}
	if fieldVal.Type().Kind() != reflect.String {
		return "", "", errors.Errorf("unsupported key type, should be string found: %s", fieldVal.Type().String())
	}

	return fieldVal.String(), jsonName, nil
}

func (s *Sink) getTopLevelKeyFromPath(keyPath string) (string, error) {
	keyPaths := strings.Split(keyPath, ".")
	if len(keyPaths) < 2 {
		return "", errors.New("invalid path, require at least one field name e.g.: .URN")
	}
	if len(keyPaths) > 2 {
		return "", errors.New("invalid path, doesn't support nested field names yet")
	}
	return keyPaths[1], nil
}

func init() {
	if err := sink.Catalog.Register("kafka", func() core.Syncer {
		return &Sink{}
	}); err != nil {
		panic(err)
	}
}
