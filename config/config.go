package config

import (
	"github.com/odpf/salt/config"
)

// Config contains the configuration for meteor.
type Config struct {
	LogLevel                    string `mapstructure:"LOG_LEVEL" default:"info"`
	StatsdEnabled               bool   `mapstructure:"STATSD_ENABLED" default:"false"`
	StatsdHost                  string `mapstructure:"STATSD_HOST" default:"localhost:8125"`
	StatsdPrefix                string `mapstructure:"STATSD_PREFIX" default:"meteor"`
	MaxRetries                  int    `mapstructure:"MAX_RETRIES" default:"5"`
	RetryInitialIntervalSeconds int    `mapstructure:"RETRY_INITIAL_INTERVAL_SECONDS" default:"5"`
	StopOnSinkError             bool   `mapstructure:"STOP_ON_SINK_ERROR" default:"false"`
}

func Load() (cfg Config, err error) {
	err = config.
		NewLoader(config.WithPath("./")).
		Load(&cfg)
	if err != nil {
		return
	}

	return
}
