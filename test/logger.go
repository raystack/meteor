package test

import (
	"io/ioutil"

	"github.com/odpf/salt/log"
)

// Logger set with writer
var Logger log.Logger = log.NewLogrus(log.LogrusWithWriter(ioutil.Discard))
