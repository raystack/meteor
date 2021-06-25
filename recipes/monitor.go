package recipes

type Monitor interface {
	RecordRun(recipe Recipe, durationInMs int, success bool)
}

type defaultMonitor struct{}

func (m *defaultMonitor) RecordRun(recipe Recipe, durationInMs int, success bool) {}
