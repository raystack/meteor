package agent

import (
	"time"

	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/log"
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
}
