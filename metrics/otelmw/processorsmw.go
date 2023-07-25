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

type ProcessorMW struct {
	next       plugins.Processor
	duration   metric.Int64Histogram
	pluginName string
	recipeName string
}

func WithProcessorMW(p plugins.Processor, pluginName, recipeName string) (plugins.Processor, error) {
	meter := otel.Meter("")

	processorDuration, err := meter.Int64Histogram("meteor.processor.duration", metric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}

	return &ProcessorMW{
		next:       p,
		duration:   processorDuration,
		pluginName: pluginName,
		recipeName: recipeName,
	}, nil
}

func (mw *ProcessorMW) Init(ctx context.Context, cfg plugins.Config) error {
	return mw.next.Init(ctx, cfg)
}

func (mw *ProcessorMW) Info() plugins.Info {
	return mw.next.Info()
}

func (mw *ProcessorMW) Validate(cfg plugins.Config) error {
	return mw.next.Validate(cfg)
}

func (mw *ProcessorMW) Process(ctx context.Context, src models.Record) (dst models.Record, err error) {
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
