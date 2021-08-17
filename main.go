package main

import (
	"fmt"
	"os"

	"github.com/odpf/meteor/cmd"
	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/metrics"

	_ "github.com/odpf/meteor/plugins/extractors"
	_ "github.com/odpf/meteor/plugins/processors"
	_ "github.com/odpf/meteor/plugins/sinks"
	"github.com/odpf/salt/log"
)

func main() {

	cfg, err := config.Load()

	if err != nil {
		fmt.Printf("ERROR: %s\n", err.Error())
		os.Exit(1)
	}

	lg := log.NewLogrus(log.LogrusWithLevel(cfg.LogLevel))

	// Setup statsd monitor to collect monitoring metrics
	var monitor *metrics.StatsdMonitor
	if cfg.StatsdEnabled {
		client, err := metrics.NewStatsdClient(cfg.StatsdHost)
		if err != nil {
			fmt.Printf("ERROR: %s\n", err.Error())
			os.Exit(1)
		}
		monitor = metrics.NewStatsdMonitor(client, cfg.StatsdPrefix)
	}

	command := cmd.New(lg, monitor)

	if err := command.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
