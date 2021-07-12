package sinks

import (
	"github.com/odpf/meteor/core/sink"
	"github.com/odpf/meteor/plugins/sinks/console"
)

func PopulateFactory(factory *sink.Factory) {
	factory.Set("console", console.New)
}
