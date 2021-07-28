package date

import (
	"context"
	"time"

	"github.com/odpf/meteor/proto/odpf/meta/common"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
)

const (
	MaxDates = 3
)

type Extractor struct {
	logger plugins.Logger
}

func (d *Extractor) Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) (err error) {
	dateCount := 0
	for {
		select {
		case <-ctx.Done():
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
}

func (d *Extractor) Process() (*common.Event, error) {
	// simulate we did some heavy workload here
	time.Sleep(500 * time.Millisecond)
	// ...
	return &common.Event{
		Action:      "hello!",
		Description: "sample message",
		Timestamp:   timestamppb.New(time.Now()),
	}, nil
}

func init() {
	if err := extractor.Catalog.Register("date", &Extractor{
		logger: plugins.Log,
	}); err != nil {
		panic(err)
	}
}
