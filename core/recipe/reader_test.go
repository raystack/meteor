package recipe_test

import (
	"os"
	"testing"

	"github.com/odpf/meteor/core/recipe"
	"github.com/stretchr/testify/assert"
)

func TestReaderRead(t *testing.T) {
	t.Run("should return error if file is not found", func(t *testing.T) {
		reader := recipe.NewReader()

		_, err := reader.Read("./wrong-path.yaml")
		assert.NotNil(t, err)
	})

	t.Run("should return error if recipe is not parsed correctly", func(t *testing.T) {
		reader := recipe.NewReader()

		_, err := reader.Read("./testdata/wrong-format.txt")
		assert.NotNil(t, err)
	})

	t.Run("should return recipe from a path given in parameter", func(t *testing.T) {
		reader := recipe.NewReader()

		rcp, err := reader.Read("./testdata/test-recipe.yaml")
		if err != nil {
			t.Fatal(err)
		}
		expectedRecipe := recipe.Recipe{
			Name: "test-recipe",
			Source: recipe.SourceRecipe{
				Type: "test-source",
				Config: map[string]interface{}{
					"foo": "bar",
				},
			},
			Sinks: []recipe.SinkRecipe{
				{
					Name: "test-sink",
				},
			},
		}

		assert.Equal(t, expectedRecipe, rcp)
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

		reader := recipe.NewReader()
		rcp, err := reader.Read("./testdata/test-recipe-variables.yaml")
		if err != nil {
			t.Fatal(err)
		}
		expectedRecipe := recipe.Recipe{
			Name: "test-recipe",
			Source: recipe.SourceRecipe{
				Type: "test-source",
				Config: map[string]interface{}{
					"username": username,
					"password": password,
				},
			},
			Sinks: []recipe.SinkRecipe{
				{
					Name: "test-sink",
				},
			},
			Processors: []recipe.ProcessorRecipe{
				{
					Name: "test-processor",
				},
			},
		}

		assert.Equal(t, expectedRecipe, rcp)
	})
}

func TestReaderReadDir(t *testing.T) {
	t.Run("should return error if directory is not found", func(t *testing.T) {
		reader := recipe.NewReader()
		_, err := reader.ReadDir("./wrong-dir")
		assert.NotNil(t, err)
	})

	t.Run("should return error if path is not a directory", func(t *testing.T) {
		reader := recipe.NewReader()
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

		reader := recipe.NewReader()
		results, err := reader.ReadDir("./testdata")
		if err != nil {
			t.Fatal(err)
		}
		expected := []recipe.Recipe{
			{
				Name: "test-recipe",
				Source: recipe.SourceRecipe{
					Type: "test-source",
					Config: map[string]interface{}{
						"username": username,
						"password": password,
					},
				},
				Sinks: []recipe.SinkRecipe{
					{
						Name: "test-sink",
					},
				},
				Processors: []recipe.ProcessorRecipe{
					{
						Name: "test-processor",
					},
				},
			},
			{
				Name: "test-recipe",
				Source: recipe.SourceRecipe{
					Type: "test-source",
					Config: map[string]interface{}{
						"foo": "bar",
					},
				},
				Sinks: []recipe.SinkRecipe{
					{
						Name: "test-sink",
					},
				},
			},
		}

		assert.Equal(t, expected, results)
	})
}
