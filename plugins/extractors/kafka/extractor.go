package kafka

import (
	"errors"
	"fmt"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/segmentio/kafka-go"
)

type Extractor struct {
	logger plugins.Logger
}

func New(logger plugins.Logger) extractor.TopicExtractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Extract(config map[string]interface{}) (result []meta.Topic, err error) {
	e.logger.Info("extracting kafka metadata...")
	broker, ok := config["broker"]
	if !ok {
		return result, errors.New("invalid config")
	}

	conn, err := kafka.Dial("tcp", fmt.Sprint(broker))
	if err != nil {
		return result, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return result, err
	}
	result = e.getTopicList(partitions)

	return result, err
}

func (e *Extractor) getTopicList(partitions []kafka.Partition) (result []meta.Topic) {
	m := map[string]struct{}{}
	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}

	for topic := range m {
		result = append(result, meta.Topic{
			Urn:  topic,
			Name: topic,
		})
	}

	return result
}
