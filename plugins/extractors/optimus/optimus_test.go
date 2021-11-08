package optimus_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/dnaeon/go-vcr/v2/recorder"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/optimus"
	"github.com/odpf/meteor/test/mocks"
	testutils "github.com/odpf/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	validConfig = map[string]interface{}{
		"host": "http://127.0.0.1:2379",
	}
)

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		extr := optimus.New(testutils.Logger, &http.Client{})
		err := extr.Init(context.TODO(), map[string]interface{}{})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should hit optimus /ping to check connection if config is valid", func(t *testing.T) {
		r, err := recorder.New("fixtures/ping")
		require.NoError(t, err)
		defer func(r *recorder.Recorder) {
			err := r.Stop()
			require.NoError(t, err)
		}(r)

		client := &http.Client{Transport: r}
		extr := optimus.New(testutils.Logger, client)
		err = extr.Init(context.TODO(), validConfig)
		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should build Job models from Optimus", func(t *testing.T) {
		r, err := recorder.New("fixtures/extract")
		require.NoError(t, err)
		defer func(r *recorder.Recorder) {
			err := r.Stop()
			require.NoError(t, err)
		}(r)

		client := &http.Client{Transport: r}
		extr := optimus.New(testutils.Logger, client)
		err = extr.Init(context.TODO(), validConfig)
		require.NoError(t, err)

		emitter := mocks.NewEmitter()
		err = extr.Extract(context.TODO(), emitter.Push)
		assert.NoError(t, err)

		actual := emitter.GetAllData()
		testutils.AssertWithJSONFile(t, "testdata/expected.json", actual)
	})
}
