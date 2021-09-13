// +build integration

package bigquery_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	_ "github.com/odpf/meteor/plugins/extractors/bigquery"
	"github.com/odpf/meteor/registry"
	"github.com/stretchr/testify/assert"
)

func TestExtract(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		extr, _ := registry.Extractors.Get("bigquery")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan models.Record)
		err := extr.Extract(ctx, map[string]interface{}{
			"wrong-config": "sample-project",
		}, extractOut)

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}
