//go:build integration
// +build integration

package snowflake_test

import (
	"context"
	"github.com/odpf/meteor/plugins/extractors/snowflake"
	"testing"

	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/test/mocks"
	"github.com/odpf/meteor/test/utils"
	_ "github.com/snowflakedb/gosnowflake" // used to register the snowflake driver
	"github.com/stretchr/testify/assert"
)

// TestInit tests the configs
func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := snowflake.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"invalid_config": "invalid_config_value",
		})
		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

// This test is supported on Superset trial version account which will get expired in 30 days,
// after that the extraction of dummy data will not be possible and test will fail.
// TestExtract tests that the extractor returns the expected result
func TestExtract(t *testing.T) {
	t.Run("should return mock-data we generated with snowflake", func(t *testing.T) {
		ctx := context.TODO()
		newExtractor := snowflake.New(utils.Logger)

		if err := newExtractor.Init(ctx, map[string]interface{}{
			"connection_url": "testing:Snowtest0512@lrwfgiz-hi47152/SNOWFLAKE_SAMPLE_DATA",
		}); err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err := newExtractor.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		var urns []string
		for _, record := range emitter.Get() {
			table := record.Data().(*assetsv1beta1.Table)
			urns = append(urns, table.Resource.Urn)

		}
		assert.Equal(t, 86, len(urns))
	})
}
