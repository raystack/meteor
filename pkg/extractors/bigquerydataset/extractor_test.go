package bigquerydataset_test

import (
	"testing"

	"github.com/odpf/meteor/pkg/extractors/bigquerydataset"
	"github.com/stretchr/testify/assert"
)

func TestExtract(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		extractor := new(bigquerydataset.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"project_id": "sample-project",
		})

		assert.NotNil(t, err)
	})
}
