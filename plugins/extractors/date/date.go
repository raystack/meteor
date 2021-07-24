package date

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"time"
)

const (
	MaxDates = 3
)

type Extractor struct {
	logger plugins.Logger
}

type Payload struct{
	Value string
}

type Payloads []Payload

func (p *Payload) ToJSON() ([]byte, error) {
	return json.Marshal(p)
}

func (p *Payload) ToProto() ([]byte, error) {
	protoC := &facets.Custom{CustomProperties: map[string]string{
		"Value": p.Value,
	}}
	return proto.Marshal(protoC)
}

func (d *Extractor) Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) (err error) {
	dateCount := 0
	for {
		select {
		case <- ctx.Done():
			// check for done if in case extraction is cancelled
			// maybe pipeline has failed down the line?
			// maybe timmed out?
			return nil
		default:
			p, err := d.Process()
			if err != nil {
				return err
			}
			out <- p

			dateCount++
			if dateCount >= MaxDates {
				return nil
			}
		}
	}
	return nil
}

func (d *Extractor) Process() (*Payload, error) {
	// simulate we did some heavy workload here
	time.Sleep(500 * time.Millisecond)
	// ...

	return &Payload{
		Value: fmt.Sprintf("Timestamp: %s", time.Now().String()),
	}, nil
}

func init () {
	if err := extractor.Catalog.Register("date", &Extractor{
		logger: plugins.Log,
	}); err != nil {
		panic(err)
	}
}

