package metrics

import (
	"fmt"
	"net"
	"strconv"

	"github.com/pkg/errors"

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
func (m *StatsdMonitor) RecordRun(run agent.Run) {
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
func (m *StatsdMonitor) RecordPlugin(recipeName, pluginName, pluginType string, success bool) {
	m.client.Increment(
		fmt.Sprintf(
			"%s.%s,recipe_name=%s,name=%s,type=%s,success=%t",
			m.prefix,
			pluginRunMetricName,
			recipeName,
			pluginName,
			pluginType,
			success,
		),
	)
}

// createMetricName creates a metric name for a given recipe and success
func (m *StatsdMonitor) createMetricName(metricName string, recipe recipe.Recipe, success bool) string {
	var successText = "false"
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
func NewStatsdClient(statsdAddress string) (c *statsd.StatsdClient, err error) {
	statsdHost, statsdPortStr, err := net.SplitHostPort(statsdAddress)
	if err != nil {
		err = errors.Wrap(err, "failed to split the network address")
		return
	}
	statsdPort, err := strconv.Atoi(statsdPortStr)
	if err != nil {
		err = errors.Wrap(err, "failed to convert port type")
		return
	}
	c = statsd.New(statsdHost, statsdPort)
	return
}
