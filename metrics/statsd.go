package metrics

import (
	"fmt"
	"net"
	"strconv"

	statsd "github.com/etsy/statsd/examples/go"
	"github.com/odpf/meteor/recipe"
)

var (
	runDurationMetricName = "runDuration"
	runMetricName         = "run"
)

// StatsdMonitor reprsents the statsd monitor.
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
func (m *StatsdMonitor) RecordRun(recipe recipe.Recipe, duration int, success bool, dataCount int) {
	m.client.Timing(
		m.createMetricName(runDurationMetricName, recipe, success, dataCount),
		int64(duration),
	)
	m.client.Increment(
		m.createMetricName(runMetricName, recipe, success, dataCount),
	)
}

// createMetricName creates a metric name for a given recipe and success
func (m *StatsdMonitor) createMetricName(metricName string, recipe recipe.Recipe, success bool, dataCount int) string {
	var successText = "false"
	if success {
		successText = "true"
	}

	return fmt.Sprintf(
		"%s.%s,name=%s,success=%s,dataCount=%d",
		m.prefix,
		metricName,
		recipe.Name,
		successText,
		dataCount,
	)
}

type statsdClient interface {
	Timing(string, int64)
	Increment(string)
}

// NewStatsdClient returns a new statsd client if the given address is valid
func NewStatsdClient(statsdAddress string) (c *statsd.StatsdClient, err error) {
	statsdHost, statsdPortStr, err := net.SplitHostPort(statsdAddress)
	if err != nil {
		return
	}
	statsdPort, err := strconv.Atoi(statsdPortStr)
	if err != nil {
		return
	}
	c = statsd.New(statsdHost, statsdPort)
	return
}
