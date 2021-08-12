//+build integration

package gcs_test

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/internal/logger"
	"github.com/odpf/meteor/plugins/extractors/gcs"
	"github.com/stretchr/testify/assert"
)

var log = logger.NewWithWriter("info", ioutil.Discard)

func TestExtract(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		err := gcs.New(log).Extract(context.TODO(), map[string]interface{}{
			"wrong-config": "sample-project",
		}, make(chan interface{}))

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})
}
