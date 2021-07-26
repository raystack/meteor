package date

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
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

func (d *Extractor) Process() (*facets.Custom, error) {
	// simulate we did some heavy workload here
	time.Sleep(500 * time.Millisecond)
	// ...

	return &facets.Custom{CustomProperties: map[string]string{
		"Value": fmt.Sprintf("Timestamp: %s", time.Now().String()),
	}}, nil
}

func init() {
	if err := extractor.Catalog.Register("date", func() core.Extractor {
		return &Extractor{
			logger: plugins.Log,
		}
	}); err != nil {
		panic(err)
	}
}
