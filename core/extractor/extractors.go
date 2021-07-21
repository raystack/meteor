package extractor

import (
	"fmt"

	"github.com/odpf/meteor/proto/odpf/meta"
)

type TableExtractor interface {
	Extract(config map[string]interface{}) (data []meta.Table, err error)
}

type TopicExtractor interface {
	Extract(config map[string]interface{}) (data []meta.Topic, err error)
}

type DashboardExtractor interface {
	Extract(config map[string]interface{}) (data []meta.Dashboard, err error)
}

type NotFoundError struct {
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find extractor \"%s\"", err.Name)
}
