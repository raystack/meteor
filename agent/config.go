package agent

import (
	"time"

	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

type Config struct {
	ExtractorFactory     *registry.ExtractorFactory
	ProcessorFactory     *registry.ProcessorFactory
	SinkFactory          *registry.SinkFactory
	Monitor              Monitor
	Logger               log.Logger
	MaxRetries           int
	RetryInitialInterval time.Duration
	StopOnSinkError      bool
	TimerFn              TimerFn
	SinkBatchSize        int
	DryRun               bool
	RecordLimit          int
}
