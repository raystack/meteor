package agent

import (
	"reflect"

	"github.com/odpf/meteor/recipe"
)

// Monitor is the interface for monitoring the agent.
type Monitor interface {
	RecordRun(recipe recipe.Recipe, durationInMs int, success bool, dataCount int)
}

// defaultMonitor is the default implementation of Monitor.
type defaultMonitor struct{}

func (m *defaultMonitor) RecordRun(recipe recipe.Recipe, durationInMs int, success bool, dataCount int) {
}

func isNilMonitor(monitor Monitor) bool {
	v := reflect.ValueOf(monitor)
	return !v.IsValid() || reflect.DeepEqual(v.Interface(), reflect.Zero(v.Type()).Interface())
}
