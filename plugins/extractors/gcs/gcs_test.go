//+build integration

package gcs_test

import (
	"context"
	"github.com/odpf/meteor/test/utils"
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/gcs"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		err := gcs.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"wrong-config": "sample-project",
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}
