package generator_test

import (
	"bytes"
	_ "embed"
	"testing"

	"github.com/goto/meteor/generator"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
)

var recipeVersions = [1]string{"v1beta1"}

func TestRecipe(t *testing.T) {
	var err error
	err = registry.Extractors.Register("test-extractor", func() plugins.Extractor {
		extr := mocks.NewExtractor()
		mockInfo := plugins.Info{
			Description: "Mock Extractor 1",
		}
		extr.On("Info").Return(mockInfo, nil).Once()
		return extr
	})
	assert.NoError(t, err)

	err = registry.Sinks.Register("test-sink", func() plugins.Syncer {
		mockSink := mocks.NewSink()
		mockInfo := plugins.Info{
			Description: "Mock Sink 1",
		}
		mockSink.On("Info").Return(mockInfo, nil).Once()
		return mockSink
	})
	assert.NoError(t, err)

	err = registry.Processors.Register("test-processor", func() plugins.Processor {
		mockProcessor := mocks.NewProcessor()
		mockInfo := plugins.Info{
			Description: "Mock Processor 1",
		}
		mockProcessor.On("Info").Return(mockInfo, nil).Once()
		return mockProcessor
	})
	assert.NoError(t, err)

	type args struct {
		p generator.RecipeParams
	}
	tests := []struct {
		name        string
		args        args
		expected    *generator.TemplateData
		expectedErr string
	}{
		{
			name: "success with minimal params",
			args: args{
				p: generator.RecipeParams{
					Name: "test-name",
				},
			},
			expected: &generator.TemplateData{
				Name:    "test-name",
				Version: recipeVersions[len(recipeVersions)-1],
			},
		},
		{
			name: "success with full params",
			args: args{
				p: generator.RecipeParams{
					Name:       "test-name",
					Source:     "test-extractor",
					Sinks:      []string{"test-sink"},
					Processors: []string{"test-processor"},
				},
			},
			expected: &generator.TemplateData{
				Name:    "test-name",
				Version: recipeVersions[len(recipeVersions)-1],
				Source: struct {
					Name         string
					Scope        string
					SampleConfig string
				}{
					Name: "test-extractor",
				},
				Sinks: map[string]string{
					"test-sink": "",
				},
				Processors: map[string]string{
					"test-processor": "",
				},
			},
		},
		{
			name: "error with invalid source",
			args: args{
				p: generator.RecipeParams{
					Name:   "test-name",
					Source: "invalid-source",
				},
			},
			expected:    nil,
			expectedErr: "provide extractor information: could not find extractor",
		},
		{
			name: "error with invalid sinks",
			args: args{
				p: generator.RecipeParams{
					Name:  "test-name",
					Sinks: []string{"invalid-sink"},
				},
			},
			expected:    nil,
			expectedErr: "provide sink information: could not find sink",
		},
		{
			name: "error with invalid processors",
			args: args{
				p: generator.RecipeParams{
					Name:       "test-name",
					Processors: []string{"invalid-processor"},
				},
			},
			expected:    nil,
			expectedErr: "provide processor information: could not find processor",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := generator.Recipe(tt.args.p)
			if tt.expectedErr != "" {
				assert.ErrorContains(t, err, tt.expectedErr)
				return
			}
			assert.NoError(t, err)

			assert.Equal(t, actual, tt.expected)
		})
	}
}

func TestRecipeWriteTo(t *testing.T) {
	type args struct {
		p generator.RecipeParams
	}
	tests := []struct {
		name           string
		args           args
		expectedWriter string
		expectedErr    string
	}{
		{
			name: "success with minimal params",
			args: args{
				p: generator.RecipeParams{
					Name: "test-name",
				},
			},
			expectedWriter: `name: test-name
version: v1beta1
source:
  name: 
  config:     
    
`,
			expectedErr: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer := &bytes.Buffer{}
			err := generator.RecipeWriteTo(tt.args.p, writer)
			if tt.expectedErr != "" {
				assert.ErrorContains(t, err, tt.expectedErr)
				return
			}
			assert.Equal(t, tt.expectedWriter, writer.String())
		})
	}
}

func TestGetRecipeVersions(t *testing.T) {
	tests := []struct {
		name     string
		expected [1]string
	}{
		{
			name:     "success",
			expected: recipeVersions,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := generator.GetRecipeVersions()
			assert.Equal(t, tt.expected, actual)
		})
	}
}
