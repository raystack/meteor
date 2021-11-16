package auditlog

import (
	"context"
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		la := New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := la.Init(ctx, map[string]interface{}{
			"wrong-config": "sample-project",
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should not return invalid config error if config is valid", func(t *testing.T) {
		la := New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := la.Init(ctx, map[string]interface{}{
			"project_id": "sample-project",
		})

		assert.NotEqual(t, plugins.InvalidConfigError{}, err)
	})
}
