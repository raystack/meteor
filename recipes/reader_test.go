package recipes_test

import (
	"testing"

	"github.com/odpf/meteor/recipes"
	"github.com/stretchr/testify/assert"
)

func TestReaderRead(t *testing.T) {
	t.Run("should return error if file is not found", func(t *testing.T) {
		reader := recipes.NewReader()

		_, err := reader.Read("./wrong-path.yaml")
		assert.NotNil(t, err)
	})

	t.Run("should return error if recipe is not parsed correctly", func(t *testing.T) {
		reader := recipes.NewReader()

		_, err := reader.Read("./testdata/wrong-format.txt")
		assert.NotNil(t, err)
	})

	t.Run("should return recipe from a path given in parameter", func(t *testing.T) {
		reader := recipes.NewReader()

		recipe, err := reader.Read("./testdata/test-recipe.yaml")
		if err != nil {
			t.Fatal(err)
		}
		expectedRecipe := recipes.Recipe{
			Name: "test-recipe",
			Source: recipes.SourceRecipe{
				Type: "test-source",
				Config: map[string]interface{}{
					"foo": "bar",
				},
			},
			Sinks: []recipes.SinkRecipe{
				{
					Name: "test-sink",
				},
			},
		}

		assert.Equal(t, expectedRecipe, recipe)
	})
}
