//+build integration

package bigquery_test

import (
	"io/ioutil"
	"testing"

	"github.com/odpf/meteor/logger"
	"github.com/odpf/meteor/plugins/extractors/bigquery"
	"github.com/stretchr/testify/assert"
)

var log = logger.NewWithWriter("info", ioutil.Discard)

func TestExtract(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		extractor := bigquery.New(log)
		_, err := extractor.Extract(map[string]interface{}{
			"project_id": "sample-project",
		})

		assert.NotNil(t, err)
	})
}
