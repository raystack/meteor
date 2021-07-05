package recipes_test

import (
	"os"
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

	t.Run("should parse variable in recipe with value from env vars prefixed with METEOR_", func(t *testing.T) {
		var (
			username = "admin"
			password = "1234"
		)
		os.Setenv("METEOR_SOURCE_USERNAME", username)
		os.Setenv("METEOR_SOURCE_PASSWORD", password)
		defer func() {
			os.Unsetenv("METEOR_SOURCE_USERNAME")
			os.Unsetenv("METEOR_SOURCE_PASSWORD")
		}()

		reader := recipes.NewReader()
		recipe, err := reader.Read("./testdata/test-recipe-variables.yaml")
		if err != nil {
			t.Fatal(err)
		}
		expectedRecipe := recipes.Recipe{
			Name: "test-recipe",
			Source: recipes.SourceRecipe{
				Type: "test-source",
				Config: map[string]interface{}{
					"username": username,
					"password": password,
				},
			},
			Sinks: []recipes.SinkRecipe{
				{
					Name: "test-sink",
				},
			},
			Processors: []recipes.ProcessorRecipe{
				{
					Name: "test-processor",
				},
			},
		}

		assert.Equal(t, expectedRecipe, recipe)
	})
}

func TestReaderReadDir(t *testing.T) {
	t.Run("should return error if directory is not found", func(t *testing.T) {
		reader := recipes.NewReader()
		_, err := reader.ReadDir("./wrong-dir")
		assert.NotNil(t, err)
	})

	t.Run("should return error if path is not a directory", func(t *testing.T) {
		reader := recipes.NewReader()
		_, err := reader.ReadDir("./testdata/wrong-format.txt")
		assert.NotNil(t, err)
	})

	t.Run("should return recipes on success", func(t *testing.T) {
		var (
			username = "admin"
			password = "1234"
		)
		os.Setenv("METEOR_SOURCE_USERNAME", username)
		os.Setenv("METEOR_SOURCE_PASSWORD", password)
		defer func() {
			os.Unsetenv("METEOR_SOURCE_USERNAME")
			os.Unsetenv("METEOR_SOURCE_PASSWORD")
		}()

		reader := recipes.NewReader()
		results, err := reader.ReadDir("./testdata")
		if err != nil {
			t.Fatal(err)
		}
		expected := []recipes.Recipe{
			{
				Name: "test-recipe",
				Source: recipes.SourceRecipe{
					Type: "test-source",
					Config: map[string]interface{}{
						"username": username,
						"password": password,
					},
				},
				Sinks: []recipes.SinkRecipe{
					{
						Name: "test-sink",
					},
				},
				Processors: []recipes.ProcessorRecipe{
					{
						Name: "test-processor",
					},
				},
			},
			{
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
			},
		}

		assert.Equal(t, expected, results)
	})
}
