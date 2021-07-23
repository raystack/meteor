//+build integration

package googlecloudstorage_test

import (
	"io/ioutil"
	"testing"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/logger"
	"github.com/odpf/meteor/plugins/extractors/googlecloudstorage"
	"github.com/stretchr/testify/assert"
)

var log = logger.NewWithWriter("info", ioutil.Discard)

func TestExtract(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		extr := googlecloudstorage.New(log)
		_, err := extr.Extract(map[string]interface{}{
			"wrong-config": "sample-project",
		})

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})
}
