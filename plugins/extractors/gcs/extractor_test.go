//+build integration

package gcs_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/gcs"
	"github.com/odpf/meteor/plugins/testutils"
	"github.com/stretchr/testify/assert"
)

func TestExtract(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		err := gcs.New(testutils.Logger).Extract(context.TODO(), map[string]interface{}{
			"wrong-config": "sample-project",
		}, make(chan interface{}))

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}
