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

type SinksMW struct {
	next       plugins.Syncer
	duration   metric.Int64Histogram
	pluginName string
	recipeName string
}

func WithSinkMW(s plugins.Syncer, pluginName, recipeName string) (plugins.Syncer, error) {
	meter := otel.Meter("")

	sinkDuration, err := meter.Int64Histogram("meteor.sink.duration", metric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}

	return &SinksMW{
		next:       s,
		duration:   sinkDuration,
		pluginName: pluginName,
		recipeName: recipeName,
	}, nil
}

func (mw *SinksMW) Init(ctx context.Context, cfg plugins.Config) error {
	return mw.next.Init(ctx, cfg)
}

func (mw *SinksMW) Info() plugins.Info {
	return mw.next.Info()
}

func (mw *SinksMW) Validate(cfg plugins.Config) error {
	return mw.next.Validate(cfg)
}

func (mw *SinksMW) Close() error {
	return mw.next.Close()
}

func (mw *SinksMW) Sink(ctx context.Context, batch []models.Record) (err error) {
	defer func(start time.Time) {
		mw.duration.Record(ctx,
			time.Since(start).Milliseconds(),
			metric.WithAttributes(
				attribute.String("recipe_name", mw.recipeName),
				attribute.String("sink", mw.pluginName),
			))
	}(time.Now())

	return mw.next.Sink(ctx, batch)
}
