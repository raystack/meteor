package kafka

import (
	"context"
	_ "embed"
	"fmt"
	"reflect"
	"strings"

	"github.com/MakeNowJust/heredoc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

//go:embed README.md
var summary string

type Config struct {
	Brokers string `mapstructure:"brokers" validate:"required"`
	Topic   string `mapstructure:"topic" validate:"required"`
	KeyPath string `mapstructure:"key_path"`
}

var info = plugins.Info{
	Description: "Send metadata to Apache Kafka topic.",
	Summary:     summary,
	Tags:        []string{"oss", "streaming"},
	SampleConfig: heredoc.Doc(`
	# Kafka broker addresses
	brokers: "localhost:9092"
	# The Kafka topic to write to
	topic: sample-topic-name
	# The path to the key field in the payload
	key_path: xxx
	`),
}

type ProtoReflector interface {
	ProtoReflect() protoreflect.Message
}

type Sink struct {
	plugins.BasePlugin
	writer *kafka.Writer
	config Config
	logger log.Logger
}

func New(logger log.Logger) plugins.Syncer {
	s := &Sink{
		logger: logger,
	}
	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)

	return s
}

func (s *Sink) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = s.BasePlugin.Init(ctx, config); err != nil {
		return err
	}

	s.writer = createWriter(s.config)

	return
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	for _, record := range batch {
		kafkaValue, err := models.RecordToJSON(record)
		if err != nil {
			return fmt.Errorf("serialize record: %w", err)
		}

		kafkaKey, err := s.buildKey(record.Entity(), s.config.KeyPath)
		if err != nil {
			return fmt.Errorf("build kafka key: %w", err)
		}

		if err := s.writer.WriteMessages(ctx, kafka.Message{
			Key:   kafkaKey,
			Value: kafkaValue,
		}); err != nil {
			return fmt.Errorf("write message: %w", err)
		}
	}

	return
}

func (s *Sink) Close() (err error) {
	return s.writer.Close()
}

// buildKey extracts a proto field from the entity to use as the Kafka message key.
func (s *Sink) buildKey(payload any, keyPath string) ([]byte, error) {
	if keyPath == "" {
		return nil, nil
	}

	fieldName, err := s.getTopLevelKeyFromPath(keyPath)
	if err != nil {
		return nil, err
	}
	keyString, keyJSONName, err := s.extractKeyFromPayload(fieldName, payload)
	if err != nil {
		return nil, err
	}

	reflector, ok := payload.(ProtoReflector)
	if !ok {
		return nil, fmt.Errorf("not a valid proto payload")
	}
	messageDescriptor := reflector.ProtoReflect().Descriptor()
	fieldDescriptor := messageDescriptor.Fields().ByJSONName(keyJSONName)
	if fieldDescriptor == nil {
		return nil, fmt.Errorf("failed to build kafka key")
	}

	dynamicMsgKey := dynamicpb.NewMessage(messageDescriptor)
	dynamicMsgKey.Set(fieldDescriptor, protoreflect.ValueOfString(keyString))
	return proto.Marshal(dynamicMsgKey)
}

func (s *Sink) extractKeyFromPayload(fieldName string, value any) (string, string, error) {
	valueOf := reflect.ValueOf(value)
	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}
	if valueOf.Kind() != reflect.Struct {
		return "", "", fmt.Errorf("invalid data")
	}

	structField, ok := valueOf.Type().FieldByName(fieldName)
	if !ok {
		return "", "", fmt.Errorf("invalid path, unknown field")
	}
	jsonName := strings.Split(structField.Tag.Get("json"), ",")[0]

	fieldVal := valueOf.FieldByName(fieldName)
	if !fieldVal.IsValid() || fieldVal.IsZero() {
		return "", "", fmt.Errorf("invalid path, unknown field")
	}
	if fieldVal.Type().Kind() != reflect.String {
		return "", "", fmt.Errorf("unsupported key type, should be string found: %s", fieldVal.Type().String())
	}

	return fieldVal.String(), jsonName, nil
}

func (s *Sink) getTopLevelKeyFromPath(keyPath string) (string, error) {
	keyPaths := strings.Split(keyPath, ".")
	if len(keyPaths) < 2 {
		return "", fmt.Errorf("invalid path, require at least one field name e.g.: .Urn")
	}
	if len(keyPaths) > 2 {
		return "", fmt.Errorf("invalid path, doesn't support nested field names yet")
	}
	return keyPaths[1], nil
}

func createWriter(config Config) *kafka.Writer {
	brokers := strings.Split(config.Brokers, ",")
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    config.Topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func init() {
	if err := registry.Sinks.Register("kafka", func() plugins.Syncer {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
