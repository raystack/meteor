package metrics

import (
	"fmt"
	"net"
	"strconv"

	"github.com/pkg/errors"

	statsd "github.com/etsy/statsd/examples/go"
	"github.com/odpf/meteor/agent"
	"github.com/odpf/meteor/recipe"
)

var (
	runDurationMetricName    = "runDuration"
	runRecordCountMetricName = "runRecordCount"
	runMetricName            = "run"
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
		m.createMetricName(runDurationMetricName, run.Recipe, run.Success, run.RecordCount),
		int64(run.DurationInMs),
	)
	m.client.Increment(
		m.createMetricName(runMetricName, run.Recipe, run.Success, run.RecordCount),
	)
	m.client.IncrementByValue(
		m.createMetricName(runRecordCountMetricName, run.Recipe, run.Success, run.RecordCount),
		run.RecordCount,
	)
}

// createMetricName creates a metric name for a given recipe and success
func (m *StatsdMonitor) createMetricName(metricName string, recipe recipe.Recipe, success bool, recordCount int) string {
	var successText = "false"
	if success {
		successText = "true"
	}

	return fmt.Sprintf(
		"%s.%s,name=%s,success=%s,records=%d",
		m.prefix,
		metricName,
		recipe.Name,
		successText,
		recordCount,
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
