package utils

import (
	"io"

	log "github.com/raystack/salt/observability/logger"
)

// Logger set with writer
var Logger log.Logger = log.NewLogrus(log.LogrusWithWriter(io.Discard))
