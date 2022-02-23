package recipe_test

import (
	"os"
	"testing"

	"github.com/odpf/meteor/recipe"
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
		t.Run("where recipe has a name", func(t *testing.T) {
			reader := recipe.NewReader()

			recipes, err := reader.Read("./testdata/testdir/test-recipe.yaml")
			if err != nil {
				t.Fatal(err)
			}
			expectedRecipes := []recipe.Recipe{
				{
					Name: "test-recipe",
					Source: recipe.PluginRecipe{
						Name: "test-source",
						Config: map[string]interface{}{
							"foo": "bar",
						},
					},
					Sinks: []recipe.PluginRecipe{
						{
							Name:   "test-sink",
							Config: map[string]interface{}{},
						},
					},
				}}

			assert.Len(t, recipes, len(expectedRecipes))
			for i, r := range recipes {
				compareRecipes(t, expectedRecipes[i], r)
			}
		})

		t.Run("where recipe does not have a name", func(t *testing.T) {
			reader := recipe.NewReader()

			recipes, err := reader.Read("./testdata/testdir/test-recipe-no-name.yaml")
			if err != nil {
				t.Fatal(err)
			}
			expectedRecipes := []recipe.Recipe{
				{
					Name:    "test-recipe-no-name",
					Version: "v1beta1",
					Source: recipe.PluginRecipe{
						Name: "test-source",
						Config: map[string]interface{}{
							"foo": "bar",
						},
					},
					Sinks: []recipe.PluginRecipe{
						{
							Name:   "test-sink",
							Config: map[string]interface{}{},
						},
					},
				}}

			assert.Len(t, recipes, len(expectedRecipes))
			for i, r := range recipes {
				compareRecipes(t, expectedRecipes[i], r)
			}
		})
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
		recipes, err := reader.Read("./testdata/testdir/test-recipe-variables.yaml")
		if err != nil {
			t.Fatal(err)
		}
		expectedRecipes := []recipe.Recipe{
			{
				Name: "test-recipe",
				Source: recipe.PluginRecipe{
					Name: "test-source",
					Config: map[string]interface{}{
						"username": username,
						"password": password,
					},
				},
				Sinks: []recipe.PluginRecipe{
					{
						Name:   "test-sink",
						Config: map[string]interface{}{},
					},
				},
				Processors: []recipe.PluginRecipe{
					{
						Name:   "test-processor",
						Config: map[string]interface{}{},
					},
				},
			}}

		assert.Len(t, recipes, len(expectedRecipes))
		for i, r := range recipes {
			compareRecipes(t, expectedRecipes[i], r)
		}
	})

	t.Run("should return error if directory is not found", func(t *testing.T) {
		reader := recipe.NewReader()
		_, err := reader.Read("./testdata/wrong-dir")
		assert.NotNil(t, err)
	})

	t.Run("should return error if path is not a directory", func(t *testing.T) {
		reader := recipe.NewReader()
		_, err := reader.Read("./testdata/wrong-format.txt")
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
		results, err := reader.Read("./testdata/testdir")
		if err != nil {
			t.Fatal(err)
		}
		expected := []recipe.Recipe{
			{
				Name: "test-recipe-no-name",
				Source: recipe.PluginRecipe{
					Name: "test-source",
					Config: map[string]interface{}{
						"foo": "bar",
					},
				},
				Sinks: []recipe.PluginRecipe{
					{
						Name:   "test-sink",
						Config: map[string]interface{}{},
					},
				},
			},
			{
				Name: "test-recipe",
				Source: recipe.PluginRecipe{
					Name: "test-source",
					Config: map[string]interface{}{
						"username": username,
						"password": password,
					},
				},
				Sinks: []recipe.PluginRecipe{
					{
						Name:   "test-sink",
						Config: map[string]interface{}{},
					},
				},
				Processors: []recipe.PluginRecipe{
					{
						Name:   "test-processor",
						Config: map[string]interface{}{},
					},
				},
			},
			{
				Name: "test-recipe",
				Source: recipe.PluginRecipe{
					Name: "test-source",
					Config: map[string]interface{}{
						"foo": "bar",
					},
				},
				Sinks: []recipe.PluginRecipe{
					{
						Name:   "test-sink",
						Config: map[string]interface{}{},
					},
				},
			},
		}

		assert.Len(t, results, len(expected))
		for i, r := range results {
			compareRecipes(t, expected[i], r)
		}
	})
}

func compareRecipes(t *testing.T, expected, actual recipe.Recipe) {
	assert.Equal(t, expected.Name, actual.Name)
	assert.Equal(t, len(expected.Sinks), len(actual.Sinks))
	assert.Equal(t, len(expected.Processors), len(actual.Processors))

	assert.Equal(t, expected.Source.Name, actual.Source.Name)
	assert.Equal(t, expected.Source.Config, actual.Source.Config)
	for i := range actual.Sinks {
		assert.Equal(t, expected.Sinks[i].Name, actual.Sinks[i].Name)
		assert.Equal(t, expected.Sinks[i].Config, actual.Sinks[i].Config)
	}
	for i := range actual.Processors {
		assert.Equal(t, expected.Processors[i].Name, actual.Processors[i].Name)
		assert.Equal(t, expected.Processors[i].Config, actual.Processors[i].Config)
	}
}
