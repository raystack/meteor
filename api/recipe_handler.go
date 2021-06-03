package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/odpf/meteor/domain"
	"github.com/odpf/meteor/recipes"
)

type RecipeHandler struct {
	recipeService *recipes.Service
}

func NewRecipeHandler(recipeService *recipes.Service) *RecipeHandler {
	return &RecipeHandler{
		recipeService: recipeService,
	}
}

func (h *RecipeHandler) Create(w http.ResponseWriter, r *http.Request) {
	var recipe domain.Recipe
	err := json.NewDecoder(r.Body).Decode(&recipe)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.recipeService.Create(recipe)
	if err != nil {
		status := http.StatusInternalServerError
		if _, ok := err.(recipes.InvalidRecipeError); ok {
			status = http.StatusBadRequest
		} else if errors.Is(err, recipes.ErrDuplicateRecipeName) {
			status = http.StatusConflict
		}

		http.Error(w, err.Error(), status)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("recipe created"))
}
