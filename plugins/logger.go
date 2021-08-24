package plugins

import "github.com/odpf/salt/log"

var (
	logger log.Logger = log.NewLogrus(log.LogrusWithLevel("INFO"))
)

func GetLog() log.Logger {
	return logger
}

func SetLog(l log.Logger) {
	logger = l
}
