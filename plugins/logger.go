package plugins

import "github.com/odpf/meteor/internal/logger"

type Logger interface {
	Debug(...interface{})
	Info(...interface{})
	Warn(...interface{})
	Error(...interface{})
}

var (
	Log = logger.New("INFO")
)
