package recipes_test

import (
	"errors"
	"testing"

	"github.com/odpf/meteor/domain"
	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/mocks"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/recipes"
	"github.com/odpf/meteor/sinks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestServiceRun(t *testing.T) {
	recipe := domain.Recipe{
		Name: "sample",
		Source: domain.SourceRecipe{
			Type: "test-extractor",
		},
		Processors: []domain.ProcessorRecipe{
			{Name: "test-processor", Config: map[string]interface{}{
				"proc-foo": "proc-bar",
			}},
		},
		Sinks: []domain.SinkRecipe{
			{Name: "mock-sink", Config: map[string]interface{}{
				"url": "http://localhost:3000/data",
			}},
		},
	}

	t.Run("should return Run on finish", func(t *testing.T) {
		extr := new(testExtractor)
		proc := new(testProcessor)
		sink := new(mockSink)
		sink.On("Sink", mock.Anything, mock.Anything).Return(nil)

		extrStore := extractors.NewStore()
		extrStore.Populate(map[string]extractors.Extractor{
			"test-extractor": extr,
		})
		procStore := processors.NewStore()
		procStore.Populate(map[string]processors.Processor{
			"test-processor": proc,
		})
		sinkStore := sinks.NewStore()
		sinkStore.Populate(map[string]sinks.Sink{
			"mock-sink": sink,
		})

		expectedRun := &domain.Run{
			Recipe: recipe,
			Tasks: []domain.Task{
				{
					Type:   domain.TaskTypeExtract,
					Name:   recipe.Source.Type,
					Config: recipe.Source.Config,
					Status: domain.TaskStatusComplete,
				},
				{
					Type:   domain.TaskTypeProcess,
					Name:   recipe.Processors[0].Name,
					Config: recipe.Processors[0].Config,
					Status: domain.TaskStatusComplete,
				},
				{
					Type:   domain.TaskTypeSink,
					Name:   recipe.Sinks[0].Name,
					Config: recipe.Sinks[0].Config,
					Status: domain.TaskStatusComplete,
				},
			},
		}

		r := recipes.NewService(&mocks.RecipeStore{}, extrStore, procStore, sinkStore)
		actual, err := r.Run(recipe)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, expectedRun.Recipe, actual.Recipe)
		assert.Equal(t, expectedRun.Tasks, actual.Tasks)
	})

	t.Run("should run extractor, processors and sinks", func(t *testing.T) {
		finalData := []map[string]interface{}{
			{
				"foo":  "bar",
				"test": true,
			},
			{
				"bar":  "foo",
				"test": true,
			},
		}

		extr := new(testExtractor)
		proc := new(testProcessor)
		sink := new(mockSink)
		sink.On("Sink", finalData, recipe.Sinks[0].Config).Return(nil)
		defer sink.AssertExpectations(t)

		extrStore := extractors.NewStore()
		extrStore.Populate(map[string]extractors.Extractor{
			"test-extractor": extr,
		})
		procStore := processors.NewStore()
		procStore.Populate(map[string]processors.Processor{
			"test-processor": proc,
		})
		sinkStore := sinks.NewStore()
		sinkStore.Populate(map[string]sinks.Sink{
			"mock-sink": sink,
		})

		service := recipes.NewService(&mocks.RecipeStore{}, extrStore, procStore, sinkStore)
		run, err := service.Run(recipe)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, finalData, run.Data)
	})
}

func TestServiceCreate(t *testing.T) {
	validRecipe := domain.Recipe{
		Name: "sample",
		Sinks: []domain.SinkRecipe{
			{Name: "mock-sink", Config: map[string]interface{}{
				"url": "http://localhost:3000/data",
			}},
		},
		Source: domain.SourceRecipe{
			Type: "test",
		},
	}

	t.Run("should return InvalidRecipe error if sinks are less than one", func(t *testing.T) {
		recipe := domain.Recipe{
			Name: "sample",
			Source: domain.SourceRecipe{
				Type: "test",
			},
		}

		recipeStore := new(mocks.RecipeStore)
		service := createRecipeService(recipeStore)

		err := service.Create(recipe)
		assert.IsType(t, recipes.InvalidRecipeError{}, err)
	})

	t.Run("should return ErrDuplicateRecipeName if recipe name already exists", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)
		recipeStore.On("Create", validRecipe).Return(recipes.ErrDuplicateRecipeName)
		defer recipeStore.AssertExpectations(t)

		service := createRecipeService(recipeStore)

		err := service.Create(validRecipe)
		assert.Equal(t, recipes.ErrDuplicateRecipeName, err)
	})

	t.Run("should return error from store", func(t *testing.T) {
		expectedErr := errors.New("test-error")

		recipeStore := new(mocks.RecipeStore)
		recipeStore.On("Create", validRecipe).Return(expectedErr)
		defer recipeStore.AssertExpectations(t)

		service := createRecipeService(recipeStore)

		err := service.Create(validRecipe)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("should not return error on success", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)
		recipeStore.On("Create", validRecipe).Return(nil)
		defer recipeStore.AssertExpectations(t)

		service := createRecipeService(recipeStore)

		err := service.Create(validRecipe)
		assert.Nil(t, err)
	})
}

func TestServiceFind(t *testing.T) {
	t.Run("should return error if recipe not found", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)
		service := createRecipeService(recipeStore)

		recipeName := "test"
		expectedErr := recipes.NotFoundError{RecipeName: recipeName}
		recipeStore.On("GetByName", recipeName).Return(domain.Recipe{}, expectedErr)
		defer recipeStore.AssertExpectations(t)

		_, err := service.Find(recipeName)

		assert.Equal(t, expectedErr, err)
	})
	t.Run("should return error from store", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)
		service := createRecipeService(recipeStore)

		recipeName := "test"
		expectedErr := errors.New("test store error")
		recipeStore.On("GetByName", recipeName).Return(domain.Recipe{}, expectedErr)
		defer recipeStore.AssertExpectations(t)

		_, err := service.Find(recipeName)

		assert.Equal(t, expectedErr, err)
	})
	t.Run("should return recipe on success", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)
		service := createRecipeService(recipeStore)

		recipeName := "test"
		expectedRecipe := domain.Recipe{
			Name: recipeName,
		}
		recipeStore.On("GetByName", recipeName).Return(expectedRecipe, nil)
		defer recipeStore.AssertExpectations(t)

		recipe, err := service.Find(recipeName)

		assert.Equal(t, expectedRecipe, recipe)
		assert.Nil(t, err)
	})
}

func createRecipeService(recipeStore recipes.Store) *recipes.Service {
	extractorStore := extractors.NewStore()
	processorStore := processors.NewStore()
	sinkStore := sinks.NewStore()

	return recipes.NewService(
		recipeStore,
		extractorStore,
		processorStore,
		sinkStore,
	)
}

type testProcessor struct{}

func (t *testProcessor) Process(data []map[string]interface{}, config map[string]interface{}) ([]map[string]interface{}, error) {
	for _, d := range data {
		d["test"] = true
	}

	return data, nil
}

type testExtractor struct{}

func (t *testExtractor) Extract(config map[string]interface{}) ([]map[string]interface{}, error) {
	data := []map[string]interface{}{
		{"foo": "bar"},
		{"bar": "foo"},
	}
	return data, nil
}

type mockSink struct {
	mock.Mock
}

func (m *mockSink) Sink(data []map[string]interface{}, config map[string]interface{}) error {
	args := m.Called(data, config)
	return args.Error(0)
}
