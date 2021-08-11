package agent

import (
	"reflect"

	"github.com/odpf/meteor/recipe"
)

type Monitor interface {
	RecordRun(recipe recipe.Recipe, durationInMs int, success bool)
}

type defaultMonitor struct{}

func (m *defaultMonitor) RecordRun(recipe recipe.Recipe, durationInMs int, success bool) {}

func isNilMonitor(monitor Monitor) bool {
	v := reflect.ValueOf(monitor)
	return !v.IsValid() || reflect.DeepEqual(v.Interface(), reflect.Zero(v.Type()).Interface())
}
