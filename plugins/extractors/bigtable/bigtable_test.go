//go:build plugins
// +build plugins

package bigtable_test

import (
	"context"
	"testing"

	"github.com/goto/meteor/plugins"
	bt "github.com/goto/meteor/plugins/extractors/bigtable"
	"github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

const (
	urnScope = "test-bigtable"
)

func TestInit(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		err := bt.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"wrong-config": "sample-project",
			},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return error if project_id is empty", func(t *testing.T) {
		err := bt.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"project_id": "",
			},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return error if service_account_base64 config is invalid", func(t *testing.T) {
		extr := bt.New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := extr.Init(ctx, plugins.Config{
			URNScope: "test-bigtable",
			RawConfig: map[string]interface{}{
				"project_id":             "google-project-id",
				"service_account_base64": "----", // invalid
			},
		})

		assert.ErrorContains(t, err, "decode Base64 encoded service account")
	})
}
