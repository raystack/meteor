package plugins

import "github.com/odpf/salt/log"

type Logger interface {
	Debug(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}

var (
	Log = log.NewLogrus(log.LogrusWithLevel("INFO"))
)
