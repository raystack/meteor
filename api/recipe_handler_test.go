package api_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/odpf/meteor/api"
	"github.com/odpf/meteor/domain"
	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/mocks"
	"github.com/odpf/meteor/processors"
	"github.com/odpf/meteor/services"
	"github.com/odpf/meteor/sinks"
	"github.com/stretchr/testify/assert"
)

func TestRecipeHandlerCreate(t *testing.T) {
	validRecipe := domain.Recipe{
		Name:   "test",
		Source: domain.SourceRecipe{Type: "test"},
		Sinks: []domain.SinkRecipe{
			{Name: "test"},
		},
	}

	t.Run("should return 400 if payload is not json", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)

		handler := createRecipeHandler(recipeStore)
		payload := "invalid-json"

		rr := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
		rw := httptest.NewRecorder()
		handler.Create(rw, rr)

		assert.Equal(t, http.StatusBadRequest, rw.Result().StatusCode)
	})

	t.Run("should return 400 if payload is invalid", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)

		handler := createRecipeHandler(recipeStore)
		payload := `{
			"name": "test"
		}`

		rr := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
		rw := httptest.NewRecorder()
		handler.Create(rw, rr)

		assert.Equal(t, http.StatusBadRequest, rw.Result().StatusCode)
	})

	t.Run("should return 409 if recipe already exists", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)
		recipeStore.On("Create", validRecipe).Return(domain.ErrDuplicateRecipeName)
		handler := createRecipeHandler(recipeStore)

		jsonBytes, err := json.Marshal(validRecipe)
		if err != nil {
			t.Error(err)
		}
		rr := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(jsonBytes))
		rw := httptest.NewRecorder()
		handler.Create(rw, rr)

		assert.Equal(t, http.StatusConflict, rw.Result().StatusCode)
	})
	t.Run("should return 500 if service returns error", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)
		recipeStore.On("Create", validRecipe).Return(errors.New("test-error"))
		handler := createRecipeHandler(recipeStore)

		jsonBytes, err := json.Marshal(validRecipe)
		if err != nil {
			t.Error(err)
		}
		rr := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(jsonBytes))
		rw := httptest.NewRecorder()
		handler.Create(rw, rr)

		assert.Equal(t, http.StatusInternalServerError, rw.Result().StatusCode)
	})
	t.Run("should return 201 on success", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)
		recipeStore.On("Create", validRecipe).Return(nil)
		handler := createRecipeHandler(recipeStore)

		jsonBytes, err := json.Marshal(validRecipe)
		if err != nil {
			t.Error(err)
		}
		rr := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(jsonBytes))
		rw := httptest.NewRecorder()
		handler.Create(rw, rr)

		assert.Equal(t, http.StatusCreated, rw.Result().StatusCode)
	})
}

func TestRecipeHandlerRun(t *testing.T) {
	t.Run("should return 400 if json is invalid", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)

		handler := createRecipeHandler(recipeStore)
		payload := "invalid-json"

		rr := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
		rw := httptest.NewRecorder()
		handler.Run(rw, rr)

		assert.Equal(t, http.StatusBadRequest, rw.Result().StatusCode)
	})

	t.Run("should return 400 if recipe name is empty", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)

		handler := createRecipeHandler(recipeStore)
		payload := `{"recipe_name": ""}`

		rr := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
		rw := httptest.NewRecorder()
		handler.Run(rw, rr)

		assert.Equal(t, http.StatusBadRequest, rw.Result().StatusCode)
	})

	t.Run("should return 404 if recipe name not found", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)

		handler := createRecipeHandler(recipeStore)
		payload := `{"recipe_name": "test"}`
		recipeStore.On("GetByName", "test").Return(domain.Recipe{}, domain.RecipeNotFoundError{})

		rr := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
		rw := httptest.NewRecorder()
		handler.Run(rw, rr)

		assert.Equal(t, http.StatusNotFound, rw.Result().StatusCode)
	})

	t.Run("should return 500 if service.Find returns any error", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)

		handler := createRecipeHandler(recipeStore)
		payload := `{"recipe_name": "test"}`
		expectedErr := errors.New("test store error")
		recipeStore.On("GetByName", "test").Return(domain.Recipe{}, expectedErr)

		rr := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
		rw := httptest.NewRecorder()
		handler.Run(rw, rr)

		assert.Equal(t, http.StatusInternalServerError, rw.Result().StatusCode)
	})

	t.Run("should return 500 if service.Run returns any error", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)

		handler := createRecipeHandler(recipeStore)
		payload := `{"recipe_name": "test"}`
		recipe := domain.Recipe{
			Name: "test",
			Source: domain.SourceRecipe{
				Type: "invalid",
			},
		}
		expectedErr := errors.New("test store error")
		recipeStore.On("GetByName", "test").Return(recipe, expectedErr)

		rr := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
		rw := httptest.NewRecorder()
		handler.Run(rw, rr)

		assert.Equal(t, http.StatusInternalServerError, rw.Result().StatusCode)
	})

	t.Run("should return 200 and run object on success", func(t *testing.T) {
		recipeStore := new(mocks.RecipeStore)

		handler := createRecipeHandler(recipeStore)
		payload := `{"recipe_name": "test"}`
		extractorName := "test"
		expectedRecipe := domain.Recipe{
			Name: "test",
			Source: domain.SourceRecipe{
				Type: extractorName,
			},
		}
		expectedResponseBody := domain.Run{
			Recipe: expectedRecipe,
			Tasks: []domain.Task{
				{
					Type:   domain.TaskTypeExtract,
					Status: domain.TaskStatusComplete,
					Name:   extractorName,
				},
			},
		}
		recipeStore.On("GetByName", "test").Return(expectedRecipe, nil)

		rr := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
		rw := httptest.NewRecorder()
		handler.Run(rw, rr)

		var actualResponseBody domain.Run
		err := json.NewDecoder(rw.Body).Decode(&actualResponseBody)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, http.StatusOK, rw.Result().StatusCode)
		assert.Equal(t, expectedResponseBody, actualResponseBody)
	})
}

type testExtractor struct{}

func (e *testExtractor) Extract(config map[string]interface{}) ([]map[string]interface{}, error) {
	return nil, nil
}

func createRecipeHandler(recipeStore domain.RecipeStore) *api.RecipeHandler {
	extractorStore := extractors.NewStore()
	extractorStore.Populate(map[string]extractors.Extractor{
		"test": &testExtractor{},
	})

	processorStore := processors.NewStore()
	sinkStore := sinks.NewStore()
	recipeService := services.NewRecipeService(
		recipeStore,
		extractorStore,
		processorStore,
		sinkStore,
	)

	return api.NewRecipeHandler(recipeService)
}
