package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/meteor/config"
	"github.com/goto/meteor/recipe"
	"github.com/goto/salt/log"
	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/contrib/samplers/probability/consistent"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"google.golang.org/grpc/encoding/gzip"
)

const gracePeriod = 5 * time.Second

func InitOtel(ctx context.Context, cfg config.Config, logger *log.Logrus, appVersion string) (func(), error) {
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithOS(),
		resource.WithHost(),
		resource.WithProcess(),
		resource.WithProcessRuntimeName(),
		resource.WithProcessRuntimeVersion(),
		resource.WithAttributes(
			semconv.ServiceName(cfg.AppName),
			semconv.ServiceVersion(appVersion),
		))
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	shutdownMetric, err := initGlobalMetrics(ctx, res, cfg, logger)
	if err != nil {
		return nil, err
	}

	shutdownTracer, err := initGlobalTracer(ctx, res, cfg, logger)
	if err != nil {
		shutdownMetric()
		return nil, err
	}

	shutdownProviders := func() {
		shutdownTracer()
		shutdownMetric()
	}

	if err := host.Start(); err != nil {
		shutdownProviders()
		return nil, err
	}

	if err := runtime.Start(); err != nil {
		shutdownProviders()
		return nil, err
	}

	return shutdownProviders, nil
}

func initGlobalMetrics(ctx context.Context, res *resource.Resource, cfg config.Config, logger *log.Logrus) (func(), error) {
	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(cfg.OtelCollectorAddr),
		otlpmetricgrpc.WithCompressor(gzip.Name),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create metric exporter: %w", err)
	}

	reader := sdkmetric.NewPeriodicReader(
		exporter,
		sdkmetric.WithInterval(15*time.Second),
	)

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(provider)

	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), gracePeriod)
		defer cancel()
		if err := provider.Shutdown(shutdownCtx); err != nil {
			logger.Error("otlp metric-provider failed to shutdown", "err", err)
		}
	}, nil
}

func initGlobalTracer(ctx context.Context, res *resource.Resource, cfg config.Config, logger *log.Logrus) (func(), error) {
	exporter, err := otlptrace.New(ctx, otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(cfg.OtelCollectorAddr),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithCompressor(gzip.Name),
	))
	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(consistent.ProbabilityBased(cfg.OtelTraceSampleProbability)),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter)),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), gracePeriod)
		defer cancel()
		if err := tracerProvider.Shutdown(shutdownCtx); err != nil {
			logger.Error("otlp trace-provider failed to shutdown", "err", err)
		}
	}, nil
}

func getSliceStringPluginNames(prs []recipe.PluginRecipe) []string {
	var res []string
	for _, pr := range prs {
		res = append(res, pr.Name)
	}

	return res
}
