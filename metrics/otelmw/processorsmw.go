package otelmw

import (
	"context"
	"time"

	"github.com/goto/meteor/models"
	"github.com/goto/meteor/plugins"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Processor struct {
	next       plugins.Processor
	duration   metric.Int64Histogram
	pluginName string
	recipeName string
}

func WithProcessor(pluginName, recipeName string) func(plugins.Processor) plugins.Processor {
	processorDuration, err := otel.Meter("github.com/goto/meteor/metrics/otelmw").
		Int64Histogram("meteor.processor.duration", metric.WithUnit("ms"))
	if err != nil {
		otel.Handle(err)
	}

	return func(p plugins.Processor) plugins.Processor {
		return &Processor{
			next:       p,
			duration:   processorDuration,
			pluginName: pluginName,
			recipeName: recipeName,
		}
	}
}

func (mw *Processor) Init(ctx context.Context, cfg plugins.Config) error {
	return mw.next.Init(ctx, cfg)
}

func (mw *Processor) Info() plugins.Info {
	return mw.next.Info()
}

func (mw *Processor) Validate(cfg plugins.Config) error {
	return mw.next.Validate(cfg)
}

func (mw *Processor) Process(ctx context.Context, src models.Record) (dst models.Record, err error) {
	defer func(start time.Time) {
		mw.duration.Record(ctx,
			time.Since(start).Milliseconds(),
			metric.WithAttributes(
				attribute.String("recipe_name", mw.recipeName),
				attribute.String("processor", mw.pluginName),
			))
	}(time.Now())

	return mw.next.Process(ctx, src)
}
