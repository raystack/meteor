//go:build plugins
// +build plugins

package bigtable_test

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/odpf/meteor/test/utils"

	"cloud.google.com/go/bigtable"
	"github.com/odpf/meteor/plugins"
	bt "github.com/odpf/meteor/plugins/extractors/bigtable"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
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

		assert.EqualError(t, err, "invalid extractor config")
	})
}
