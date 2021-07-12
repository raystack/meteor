package kafka

import (
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Extractor struct{}

func (e *Extractor) Extract(config map[string]interface{}) (result []map[string]interface{}, err error) {
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

func (e *Extractor) getTopicList(partitions []kafka.Partition) (result []map[string]interface{}) {
	m := map[string]struct{}{}
	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}

	for topic := range m {
		result = append(result, map[string]interface{}{
			"topic": topic,
		})
	}

	return result
}
