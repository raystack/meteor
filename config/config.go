package config

import (
	"fmt"

	"github.com/jeremywohl/flatten"
	"github.com/mcuadros/go-defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

type Config struct {
	LogLevel      string `mapstructure:"LOG_LEVEL" default:"info"`
	StatsdEnabled bool   `mapstructure:"STATSD_ENABLED" default:"false"`
	StatsdHost    string `mapstructure:"STATSD_HOST" default:"localhost:8125"`
	StatsdPrefix  string `mapstructure:"STATSD_PREFIX" default:"meteor"`
}

// Load returns application configuration
func Load() (c Config, err error) {
	viper.SetConfigName("config")
	viper.AddConfigPath("./")
	viper.AddConfigPath("../")
	viper.SetConfigType("yaml")
	viper.AutomaticEnv()

	err = viper.ReadInConfig()

	if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
		return c, err
	}

	bindEnvVars()
	defaults.SetDefaults(&c)
	err = viper.Unmarshal(&c)
	if err != nil {
		return c, fmt.Errorf("unable to unmarshal config to struct: %v", err)
	}

	return
}

func bindEnvVars() {
	keys, err := getFlattenedStructKeys(Config{})
	if err != nil {
		panic(err)
	}

	// Bind each conf fields to environment vars
	for key := range keys {
		err := viper.BindEnv(keys[key])
		if err != nil {
			panic(err)
		}
	}
}

func getFlattenedStructKeys(config Config) ([]string, error) {
	var structMap map[string]interface{}
	err := mapstructure.Decode(config, &structMap)
	if err != nil {
		return nil, err
	}

	flat, err := flatten.Flatten(structMap, "", flatten.DotStyle)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(flat))
	for k := range flat {
		keys = append(keys, k)
	}

	return keys, nil
}
