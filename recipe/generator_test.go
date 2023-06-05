package recipe_test

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/goto/meteor/recipe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestFromTemplate(t *testing.T) {
	t.Run("should throw error for invalid template path", func(t *testing.T) {
		templatePath := "./testdata/template.yaml"
		outputDir := "./test/temp"
		bytes, err := os.ReadFile("./testdata/generator/data-3.yaml")
		if err != nil {
			fmt.Println(fmt.Errorf("error reading data: %w", err))
			return
		}

		var data []recipe.TemplateData
		if err := yaml.Unmarshal(bytes, &data); err != nil {
			fmt.Println(fmt.Errorf("error parsing data: %w", err))
			return
		}

		err = recipe.FromTemplate(recipe.TemplateConfig{
			TemplateFilePath: templatePath,
			OutputDirPath:    outputDir,
			Data:             data,
		})
		assert.Error(t, err)
	})

	t.Run("should output recipe files using template to output directory", func(t *testing.T) {
		templatePath := "./testdata/generator/template.yaml"
		outputDir := "./testdata/generator/temp"

		t.Run("when recipe has a name", func(t *testing.T) {
			bytes, err := os.ReadFile("./testdata/generator/data-1-2.yaml")
			if err != nil {
				fmt.Println(fmt.Errorf("error reading data: %w", err))
				return
			}

			var data []recipe.TemplateData
			if err := yaml.Unmarshal(bytes, &data); err != nil {
				fmt.Println(fmt.Errorf("error parsing data: %w", err))
				return
			}

			cleanDir(t, outputDir)
			defer cleanDir(t, outputDir)

			err = recipe.FromTemplate(recipe.TemplateConfig{
				TemplateFilePath: templatePath,
				OutputDirPath:    outputDir,
				Data:             data,
			})
			require.NoError(t, err)

			assertRecipeFile(t,
				"./testdata/generator/expected.yaml",
				path.Join(outputDir, data[0].FileName+".yaml"),
			)
			assertRecipeFile(t,
				"./testdata/generator/expected-2.yaml",
				path.Join(outputDir, data[1].FileName+".yaml"),
			)
		})

		t.Run("when recipe does not have a name", func(t *testing.T) {
			bytes, err := os.ReadFile("./testdata/generator/data-3.yaml")
			if err != nil {
				fmt.Println(fmt.Errorf("error reading data: %w", err))
				return
			}

			var data []recipe.TemplateData
			if err := yaml.Unmarshal(bytes, &data); err != nil {
				fmt.Println(fmt.Errorf("error parsing data: %w", err))
				return
			}

			cleanDir(t, outputDir)
			defer cleanDir(t, outputDir)

			err = recipe.FromTemplate(recipe.TemplateConfig{
				TemplateFilePath: templatePath,
				OutputDirPath:    outputDir,
				Data:             data,
			})
			require.NoError(t, err)

			assertRecipeFile(t,
				"./testdata/generator/expected-3.yaml",
				path.Join(outputDir, data[0].FileName+".yaml"),
			)
		})
	})
}

func cleanDir(t *testing.T, dirPath string) {
	err := os.RemoveAll(dirPath)
	require.NoError(t, err)
}

func assertRecipeFile(t *testing.T, expectedPath, actualPath string) {
	expectedBytes, err := os.ReadFile(expectedPath)
	require.NoError(t, err)
	actualBytes, err := os.ReadFile(actualPath)
	require.NoError(t, err)
	assert.Equal(t, string(expectedBytes), string(actualBytes))
}
