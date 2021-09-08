package agent

// Config contains the configuration for the agent.
type Config struct {
	LogLevel      string `mapstructure:"LOG_LEVEL" default:"info"`
	StatsdEnabled bool   `mapstructure:"STATSD_ENABLED" default:"false"`
	StatsdHost    string `mapstructure:"STATSD_HOST" default:"localhost:8125"`
	StatsdPrefix  string `mapstructure:"STATSD_PREFIX" default:"meteor"`
}
