package agent

import (
	"time"

	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
)

type Config struct {
	ExtractorFactory     *registry.ExtractorFactory
	ProcessorFactory     *registry.ProcessorFactory
	SinkFactory          *registry.SinkFactory
	Monitor              Monitor
	Logger               log.Logger
	MaxRetries           int
	RetryInitialInterval time.Duration
}
