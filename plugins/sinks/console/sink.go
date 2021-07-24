package console

import (
	"context"
	"errors"
	"fmt"
	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/core/sink"
)

type Sink struct{}

func New() core.Syncer {
	return new(Sink)
}

func (s *Sink) Sink(ctx context.Context, config map[string]interface{}, out <-chan interface{}) (err error) {
	for val := range out {
		if err := s.Process(val); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) Process(value interface{}) error {
	jsonParcel, ok := value.(core.JSONCodec);
	if !ok {
		return errors.New("unsupported payload")
	}

	jsonBytes, err := jsonParcel.ToJSON()
	if err != nil {
		return err
	}
	fmt.Println(string(jsonBytes))

	return nil
}

func init() {
	if err := sink.Catalog.Register("console", New()); err != nil {
		panic(err)
	}
}
