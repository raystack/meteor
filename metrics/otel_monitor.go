package metrics

import (
	"context"

	"github.com/goto/meteor/agent"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// OtelMonitor represents the otel monitor.
type OtelMonitor struct {
	recipeDuration   metric.Float64Histogram
	extractorRetries metric.Int64Counter
	assetsExtracted  metric.Int64Counter
	sinkRetries      metric.Int64Counter
}

func NewOtelMonitor() *OtelMonitor {
	// init meters
	meter := otel.Meter("github.com/goto/meteor/metrics")
	recipeDuration, err := meter.Float64Histogram("meteor.recipe.duration", metric.WithUnit("s"))
	handleOtelErr(err)

	extractorRetries, err := meter.Int64Counter("meteor.extractor.retries")
	handleOtelErr(err)

	assetsExtracted, err := meter.Int64Counter("meteor.assets.extracted")
	handleOtelErr(err)

	sinkRetries, err := meter.Int64Counter("meteor.sink.retries")
	handleOtelErr(err)

	return &OtelMonitor{
		recipeDuration:   recipeDuration,
		extractorRetries: extractorRetries,
		assetsExtracted:  assetsExtracted,
		sinkRetries:      sinkRetries,
	}
}

// RecordRun records a run behavior
func (m *OtelMonitor) RecordRun(ctx context.Context, run agent.Run) {
	m.recipeDuration.Record(ctx,
		float64(run.DurationInMs)/1000.0,
		metric.WithAttributes(
			attribute.String("recipe_name", run.Recipe.Name),
			attribute.String("extractor", run.Recipe.Source.Name),
			attribute.StringSlice("processors", getSliceStringPluginNames(run.Recipe.Processors)),
			attribute.StringSlice("sinks", getSliceStringPluginNames(run.Recipe.Sinks)),
			attribute.Bool("success", run.Success),
		))

	m.extractorRetries.Add(ctx,
		int64(run.ExtractorRetries),
		metric.WithAttributes(
			attribute.String("recipe_name", run.Recipe.Name),
			attribute.String("extractor", run.Recipe.Source.Name),
		))

	m.assetsExtracted.Add(ctx,
		int64(run.AssetsExtracted),
		metric.WithAttributes(
			attribute.String("recipe_name", run.Recipe.Name),
			attribute.String("extractor", run.Recipe.Source.Name),
			attribute.StringSlice("processors", getSliceStringPluginNames(run.Recipe.Processors)),
			attribute.StringSlice("sinks", getSliceStringPluginNames(run.Recipe.Sinks)),
		))
}

// RecordPlugin records a individual plugin behavior in a run, this is being handled in otelmw
func (*OtelMonitor) RecordPlugin(context.Context, agent.PluginInfo) {}

func (m *OtelMonitor) RecordSinkRetryCount(ctx context.Context, pluginInfo agent.PluginInfo) {
	m.sinkRetries.Add(ctx,
		1,
		metric.WithAttributes(
			attribute.String("recipe_name", pluginInfo.RecipeName),
			attribute.String("sink", pluginInfo.PluginName),
			attribute.Int64("batch_size", int64(pluginInfo.BatchSize)),
		))
}

func handleOtelErr(err error) {
	if err != nil {
		otel.Handle(err)
	}
}
