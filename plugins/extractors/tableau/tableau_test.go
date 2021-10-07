//+build integration

package tableau_test

import (
	"context"
	"testing"

	"github.com/bmizerany/assert"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/tableau"
	"github.com/odpf/meteor/test"
)

var (
	user = "gk74533@gmail.com"
	pass = "MeteorTest@2021"
	host = "https:localhost:4000/"
)

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		extr := tableau.New(test.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, map[string]interface{}{
			"wrong-config": "sample-project",
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
	t.Run("should not return invalid config error if config is valid", func(t *testing.T) {
		extr := tableau.New(test.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, map[string]interface{}{
			"user_id":  user,
			"host":     host,
			"password": pass,
		})

		assert.NotEqual(t, plugins.InvalidConfigError{}, err)
	})
}

func TestExtract(t *testing.T) {

}
