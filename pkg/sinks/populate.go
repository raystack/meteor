package sinks

import (
	"github.com/odpf/meteor/pkg/sinks/console"
	"github.com/odpf/meteor/sinks"
)

func PopulateStore(store *sinks.Store) {
	store.Set("console", new(console.Sink))
}
