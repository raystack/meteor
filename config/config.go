package config

import (
	"errors"
	"fmt"

	"github.com/goto/salt/config"
)

// Config contains the configuration for meteor.
type Config struct {
	AppName                     string  `mapstructure:"APP_NAME" default:"meteor"`
	LogLevel                    string  `mapstructure:"LOG_LEVEL" default:"info"`
	MaxRetries                  int     `mapstructure:"MAX_RETRIES" default:"5"`
	RetryInitialIntervalSeconds int     `mapstructure:"RETRY_INITIAL_INTERVAL_SECONDS" default:"5"`
	StopOnSinkError             bool    `mapstructure:"STOP_ON_SINK_ERROR" default:"false"`
	StatsdEnabled               bool    `mapstructure:"STATSD_ENABLED" default:"false"`
	StatsdHost                  string  `mapstructure:"STATSD_HOST" default:"localhost:8125"`
	OtelEnabled                 bool    `mapstructure:"OTEL_ENABLED" default:"false"`
	OtelCollectorAddr           string  `mapstructure:"OTEL_COLLECTOR_ADDR" default:"localhost:4317"`
	OtelTraceSampleProbability  float64 `mapstructure:"OTEL_TRACE_SAMPLE_PROBABILITY" default:"1"`
}

func Load(configFile string) (Config, error) {
	var cfg Config
	err := config.NewLoader(config.WithFile(configFile)).
		Load(&cfg)
	if err != nil {
		if errors.As(err, &config.ConfigFileNotFoundError{}) {
			fmt.Println(err)
			return cfg, nil
		}
		return Config{}, err
	}

	return cfg, nil
}
