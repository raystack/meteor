package recipes_test

import (
	"testing"

	"github.com/odpf/meteor/recipes"
	"github.com/stretchr/testify/assert"
)

func TestReaderRead(t *testing.T) {
	dirname := "./test_recipes"
	t.Run("should return error if path is not a directory", func(t *testing.T) {
		reader := recipes.NewReader("./wrong-dir-path")
		_, err := reader.Read()

		assert.NotNil(t, err)
	})

	t.Run("should read all files from designated folder", func(t *testing.T) {
		reader := recipes.NewReader(dirname)
		recipes, err := reader.Read()
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, 3, len(recipes))
	})

	t.Run("should map fields to recipe correctly", func(t *testing.T) {
		reader := recipes.NewReader(dirname)
		recipeList, err := reader.Read()
		if err != nil {
			t.Error(err)
		}

		recp := recipeList[0]

		assert.Equal(t, "sample-1", recp.Name)
		assert.Equal(t, recipes.Source{
			Type: "kafka",
			Config: map[string]interface{}{
				"broker": "localhost:9092",
			},
		}, recp.Source)

		assert.Equal(t, 1, len(recp.Sinks))
		assert.Equal(t, recipes.Sink{
			Name: "http",
			Config: map[string]interface{}{
				"method": "POST",
				"url":    "http://localhost:9090/metadata",
			},
		}, recp.Sinks[0])

		assert.Equal(t, 1, len(recp.Processors))
		assert.Equal(t, recipes.Processor{
			Name: "sample-processors",
			Config: map[string]interface{}{
				"foo": "bar",
			},
		}, recp.Processors[0])
	})
}
