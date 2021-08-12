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

type StatsdMonitor struct {
	client statsdClient
	prefix string
}

func NewStatsdMonitor(client statsdClient, prefix string) *StatsdMonitor {
	return &StatsdMonitor{
		client: client,
		prefix: prefix,
	}
}

func (m *StatsdMonitor) RecordRun(recipe recipe.Recipe, duration int, success bool) {
	m.client.Timing(
		m.createMetricName(runDurationMetricName, recipe, success),
		int64(duration),
	)
	m.client.Increment(
		m.createMetricName(runMetricName, recipe, success),
	)
}

func (m *StatsdMonitor) createMetricName(metricName string, recipe recipe.Recipe, success bool) string {
	var successText = "false"
	if success {
		successText = "true"
	}

	return fmt.Sprintf(
		"%s.%s,name=%s,success=%s",
		m.prefix,
		metricName,
		recipe.Name,
		successText,
	)
}

type statsdClient interface {
	Timing(string, int64)
	Increment(string)
}

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
