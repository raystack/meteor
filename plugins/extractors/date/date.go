package date

import (
	"context"
	_ "embed" // used to print the embedded assets
	"time"

	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/registry"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

// maxDates is the maximum number of dates to extract
const maxDates = 3

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	logger log.Logger
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Print current date from system",
		SampleConfig: "",
		Summary:      summary,
		Tags:         []string{"system,extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return nil
}

// Extract checks if the event contains a date and returns it
func (e *Extractor) Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) (err error) {
	dateCount := 0
	for {
		select {
		case <-ctx.Done():
			// check for done if in case extraction is cancelled
			// maybe pipeline has failed down the line?
			// maybe timmed out?
			return nil
		default:
			p, err := e.Process()
			if err != nil {
				return err
			}
			out <- p

			dateCount++
			if dateCount >= maxDates {
				return nil
			}
		}
	}
}

// Process returns the current date
func (e *Extractor) Process() (*common.Event, error) {
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
