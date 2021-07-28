package console

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/core/sink"
	"github.com/odpf/meteor/plugins"
)

type Sink struct {
	logger plugins.Logger
}

func New() core.Syncer {
	return new(Sink)
}

func (s *Sink) Sink(ctx context.Context, config map[string]interface{}, out <-chan interface{}) (err error) {
	for val := range out {
		if err := s.process(val); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) process(value interface{}) error {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	fmt.Println(string(jsonBytes))
	return nil
}

func init() {
	if err := sink.Catalog.Register("console", func() core.Syncer {
		return &Sink{
			logger: plugins.Log,
		}
	}); err != nil {
		panic(err)
	}
}
