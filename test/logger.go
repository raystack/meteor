package test

import (
	"io/ioutil"

	"github.com/odpf/salt/log"
)

var Logger log.Logger = log.NewLogrus(log.LogrusWithWriter(ioutil.Discard))
