package metrics

import (
	"context"
	"fmt"
	"net"
	"strconv"

	statsd "github.com/etsy/statsd/examples/go"
	"github.com/goto/meteor/agent"
	"github.com/goto/meteor/recipe"
)

const (
	runDurationMetricName    = "runDuration"
	runRecordCountMetricName = "runRecordCount"
	runMetricName            = "run"
	pluginRunMetricName      = "runPlugin"
)

// StatsdMonitor represents the statsd monitor.
type StatsdMonitor struct {
	client statsdClient
	prefix string
}

// NewStatsdMonitor creates a new StatsdMonitor
func NewStatsdMonitor(client statsdClient, prefix string) *StatsdMonitor {
	return &StatsdMonitor{
		client: client,
		prefix: prefix,
	}
}

// RecordRun records a run behavior
func (m *StatsdMonitor) RecordRun(_ context.Context, run agent.Run) {
	m.client.Timing(
		m.createMetricName(runDurationMetricName, run.Recipe, run.Success),
		int64(run.DurationInMs),
	)
	m.client.Increment(
		m.createMetricName(runMetricName, run.Recipe, run.Success),
	)
	m.client.IncrementByValue(
		m.createMetricName(runRecordCountMetricName, run.Recipe, run.Success),
		run.RecordCount,
	)
}

// RecordPlugin records a individual plugin behavior in a run
func (m *StatsdMonitor) RecordPlugin(_ context.Context, pluginInfo agent.PluginInfo) {
	m.client.Increment(
		fmt.Sprintf(
			"%s.%s,recipe_name=%s,name=%s,type=%s,success=%t",
			m.prefix,
			pluginRunMetricName,
			pluginInfo.RecipeName,
			pluginInfo.PluginName,
			pluginInfo.PluginType,
			pluginInfo.Success,
		),
	)
}

func (*StatsdMonitor) RecordSinkRetryCount(context.Context, agent.PluginInfo) {}

// createMetricName creates a metric name for a given recipe and success
func (m *StatsdMonitor) createMetricName(metricName string, recipe recipe.Recipe, success bool) string {
	successText := "false"
	if success {
		successText = "true"
	}

	return fmt.Sprintf(
		"%s.%s,name=%s,success=%s,extractor=%s",
		m.prefix,
		metricName,
		recipe.Name,
		successText,
		recipe.Source.Name,
	)
}

type statsdClient interface {
	Timing(string, int64)
	Increment(string)
	IncrementByValue(string, int)
}

// NewStatsdClient returns a new statsd client if the given address is valid
func NewStatsdClient(statsdAddress string) (*statsd.StatsdClient, error) {
	host, portStr, err := net.SplitHostPort(statsdAddress)
	if err != nil {
		return nil, fmt.Errorf("split the network address: %w", err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("convert port type: %w", err)
	}

	return statsd.New(host, port), nil
}
