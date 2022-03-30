//go:build plugins
// +build plugins

package bigtable_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/test/utils"

	"github.com/odpf/meteor/plugins"
	bt "github.com/odpf/meteor/plugins/extractors/bigtable"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		err := bt.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"wrong-config": "sample-project",
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return error if project_id is empty", func(t *testing.T) {
		err := bt.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"project_id": "",
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}
