package auditlog

import (
	"context"
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("should return error if failed to init client", func(t *testing.T) {
		la := New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := la.Init(ctx, Config{
			ProjectID:          "---",
			ServiceAccountJSON: "---",
		})

		assert.EqualError(t, err, "failed to create logadmin client: client is nil, failed initiating client")
	})

	t.Run("should not return error if init client is success", func(t *testing.T) {
		la := New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := la.Init(ctx, Config{})

		assert.NotEqual(t, plugins.InvalidConfigError{}, err)
	})
}
