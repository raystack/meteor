package sinks

import (
	"github.com/odpf/meteor/pkg/sinks/console"
	"github.com/odpf/meteor/sinks"
)

func PopulateFactory(factory *sinks.Factory) {
	factory.Set("console", console.New)
}
