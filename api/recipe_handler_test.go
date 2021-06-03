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
	"github.com/odpf/meteor/recipes"
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
		recipeStore.On("Create", validRecipe).Return(recipes.ErrDuplicateRecipeName)
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

func createRecipeHandler(recipeStore recipes.Store) *api.RecipeHandler {
	extractorStore := extractors.NewStore()
	processorStore := processors.NewStore()
	sinkStore := sinks.NewStore()
	recipeService := recipes.NewService(
		recipeStore,
		extractorStore,
		processorStore,
		sinkStore,
	)

	return api.NewRecipeHandler(recipeService)
}
