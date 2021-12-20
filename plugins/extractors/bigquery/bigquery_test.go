//go:build integration
// +build integration

package bigquery_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/test/utils"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/bigquery"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		extr := bigquery.New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, map[string]interface{}{
			"wrong-config": "sample-project",
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
	t.Run("should not return invalid config error if config is valid", func(t *testing.T) {
		extr := bigquery.New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, map[string]interface{}{
			"project_id": "sample-project",
		})

		assert.NotEqual(t, plugins.InvalidConfigError{}, err)
	})
}
