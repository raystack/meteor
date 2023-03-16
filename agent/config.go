package agent

import (
	"time"

	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
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
