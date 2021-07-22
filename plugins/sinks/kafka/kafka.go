package kafka

import (
	"context"
	"errors"
	"github.com/odpf/meteor/core"
)

type KafkaSink struct {
}

// TODO
func (c *KafkaSink) Sink(ctx context.Context, config map[string]interface{}, out <-chan interface{}) (err error) {
	for raw := range out {
		if _, ok := raw.(core.ProtoCodec); !ok {
			return errors.New("not cool")
		}

		var data []interface{}
		// use stencil to decode into dynamicpb if needed

		for _ = range data {
			// push to kafka
		}
	}
	return nil
}
