package agent

import (
	"reflect"
)

// Monitor is the interface for monitoring the agent.
type Monitor interface {
	RecordRun(run Run)
	RecordSink(recipeName, sourceName string, sinkRun PluginRun)
}

// defaultMonitor is the default implementation of Monitor.
type defaultMonitor struct{}

func (m *defaultMonitor) RecordRun(run Run) {
}

func (m *defaultMonitor) RecordSink(recipeName, sourceName string, sinkRun PluginRun) {
}

func isNilMonitor(monitor Monitor) bool {
	v := reflect.ValueOf(monitor)
	return !v.IsValid() || reflect.DeepEqual(v.Interface(), reflect.Zero(v.Type()).Interface())
}
