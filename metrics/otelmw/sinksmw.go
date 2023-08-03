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

type Sinks struct {
	next       plugins.Syncer
	duration   metric.Int64Histogram
	pluginName string
	recipeName string
}

func WithSink(pluginName, recipeName string) func(plugins.Syncer) plugins.Syncer {
	sinkDuration, err := otel.Meter("github.com/goto/meteor/metrics/otelmw").
		Int64Histogram("meteor.sink.duration", metric.WithUnit("ms"))
	if err != nil {
		otel.Handle(err)
	}

	return func(s plugins.Syncer) plugins.Syncer {
		return &Sinks{
			next:       s,
			duration:   sinkDuration,
			pluginName: pluginName,
			recipeName: recipeName,
		}
	}
}

func (mw *Sinks) Init(ctx context.Context, cfg plugins.Config) error {
	return mw.next.Init(ctx, cfg)
}

func (mw *Sinks) Info() plugins.Info {
	return mw.next.Info()
}

func (mw *Sinks) Validate(cfg plugins.Config) error {
	return mw.next.Validate(cfg)
}

func (mw *Sinks) Close() error {
	return mw.next.Close()
}

func (mw *Sinks) Sink(ctx context.Context, batch []models.Record) (err error) {
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
