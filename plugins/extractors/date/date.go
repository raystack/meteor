package date

import (
	"context"
	_ "embed"
	"time"

	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/registry"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

const (
	MaxDates = 3
)

type Extractor struct {
	logger log.Logger
}

func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Print current date from system",
		SampleConfig: "",
		Summary:      summary,
		Tags:         []string{"system,extractor"},
	}
}

func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return nil
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
	if err := registry.Extractors.Register("date", func() plugins.Extractor {
		return &Extractor{
			logger: plugins.GetLog(),
		}
	}); err != nil {
		panic(err)
	}
}
