package extractors

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

type KafkaExtractor struct{}

func (e *KafkaExtractor) Extract(config map[string]interface{}) (result []map[string]interface{}, err error) {
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		return result, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return result, err
	}
	m := map[string]struct{}{}
	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}

	for topic, val := range m {
		fmt.Printf("%+v\n", val)
		result = append(result, map[string]interface{}{
			"topic": topic,
		})
	}

	return result, err
}
