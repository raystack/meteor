package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/odpf/meteor/cmd"
	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/metrics"
	"github.com/odpf/meteor/plugins"

	_ "github.com/odpf/meteor/plugins/extractors"
	_ "github.com/odpf/meteor/plugins/processors"
	_ "github.com/odpf/meteor/plugins/sinks"
	"github.com/odpf/salt/cmdx"
	"github.com/odpf/salt/log"
)

const (
	exitOK    = 0
	exitError = 1
)

func main() {
	cfg, err := config.Load()
	if err != nil {
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

	// Execute the root command
	root := cmd.New(lg, monitor, cfg)
	cmd, err := root.ExecuteC()

	if err == nil {
		return
	}

	if cmdx.IsCmdErr(err) {
		if !strings.HasSuffix(err.Error(), "\n") {
			fmt.Println()
		}
		fmt.Println(cmd.UsageString())
		os.Exit(exitOK)
	}

	fmt.Println(err)
	os.Exit(exitError)
}
