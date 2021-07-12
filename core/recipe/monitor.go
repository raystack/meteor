package recipe

import "reflect"

type Monitor interface {
	RecordRun(recipe Recipe, durationInMs int, success bool)
}

type defaultMonitor struct{}

func (m *defaultMonitor) RecordRun(recipe Recipe, durationInMs int, success bool) {}

func isNilMonitor(monitor Monitor) bool {
	v := reflect.ValueOf(monitor)
	return !v.IsValid() || reflect.DeepEqual(v.Interface(), reflect.Zero(v.Type()).Interface())
}
