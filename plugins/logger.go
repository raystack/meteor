package plugins

import "github.com/goto/salt/log"

var logger log.Logger = log.NewLogrus(log.LogrusWithLevel("INFO"))

// GetLog returns the logger
func GetLog() log.Logger {
	return logger
}

// SetLog sets the logger
func SetLog(l log.Logger) {
	logger = l
}
