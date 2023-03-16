package utils

import (
	"io"

	"github.com/goto/salt/log"
)

// Logger set with writer
var Logger log.Logger = log.NewLogrus(log.LogrusWithWriter(io.Discard))
