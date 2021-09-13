package plugins

import "github.com/odpf/meteor/models"

type Emitter interface {
	// Emit will be used to publish extracted data.
	Emit(models.Record)
}
