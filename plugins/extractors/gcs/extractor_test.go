//+build integration

package gcs_test

import (
	"io/ioutil"
	"testing"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/logger"
	"github.com/odpf/meteor/plugins/extractors/gcs"
	"github.com/stretchr/testify/assert"
)

var log = logger.NewWithWriter("info", ioutil.Discard)

func TestExtract(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		extr, _ := extractor.Catalog.Get("gcs")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan interface{})

		err := extr.Extract(ctx, map[string]interface{}{
			"wrong-config": "sample-project",
		}, extractOut)

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})
}
