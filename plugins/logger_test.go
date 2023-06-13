//go:build plugins
// +build plugins

package plugins_test

import (
	"testing"

	"github.com/goto/meteor/plugins"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
)

func TestSetGetLog(t *testing.T) {
	tests := []struct {
		name   string
		logger log.Logger
	}{
		{
			name:   "should set and get logger",
			logger: log.NewLogrus(log.LogrusWithLevel("INFO")),
		},
		{
			name:   "with nil logger",
			logger: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugins.SetLog(tt.logger)
			assert.Equal(t, tt.logger, plugins.GetLog())
		})
	}
}
