package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/odpf/meteor/agent"
	"github.com/odpf/meteor/cmd"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/salt/config"

	_ "github.com/odpf/meteor/plugins/extractors"
	_ "github.com/odpf/meteor/plugins/processors"
	_ "github.com/odpf/meteor/plugins/sinks"
	"github.com/odpf/salt/log"
)

const (
	exitOK    = 0
	exitError = 1
)

func main() {

	var cfg agent.Config
	l := config.NewLoader()

	if err := l.Load(&cfg); err != nil {
		fmt.Printf("ERROR: %s\n", err.Error())
		os.Exit(1)
	}

	lg := log.NewLogrus(log.LogrusWithLevel(cfg.LogLevel))
	plugins.SetLog(lg)

	// Setup statsd monitor to collect monitoring metrics
	var monitor *metrics.StatsdMonitor
	if cfg.StatsdEnabled {
		client, err := metrics.NewStatsdClient(cfg.StatsdHost)
		if err != nil {
			fmt.Printf("ERROR: %s\n", err.Error())
			os.Exit(exitError)
		}
		monitor = metrics.NewStatsdMonitor(client, cfg.StatsdPrefix)
	}

	command := cmd.New(lg, monitor)

	if err := command.Execute(); err != nil {
		if strings.HasPrefix(err.Error(), "unknown command ") {
			if !strings.HasSuffix(err.Error(), "\n") {
				fmt.Println()
			}
			fmt.Println(command.UsageString())
			os.Exit(exitOK)
		} else {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(exitError)
		}
	}
}
