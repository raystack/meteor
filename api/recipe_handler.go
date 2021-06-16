package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/odpf/meteor/domain"
	"github.com/odpf/meteor/services"
)

type RecipeRunRequest struct {
	RecipeName string `json:"recipe_name"`
}

type RecipeHandler struct {
	recipeService *services.RecipeService
}

func NewRecipeHandler(recipeService *services.RecipeService) *RecipeHandler {
	return &RecipeHandler{
		recipeService: recipeService,
	}
}

func (h *RecipeHandler) Create(w http.ResponseWriter, r *http.Request) {
	var recipe domain.Recipe
	err := decodeAndValidate(r.Body, "CreateRecipeRequest", &recipe)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.recipeService.Create(recipe)
	if err != nil {
		status := http.StatusInternalServerError
		if _, ok := err.(domain.InvalidRecipeError); ok {
			status = http.StatusBadRequest
		} else if errors.Is(err, domain.ErrDuplicateRecipeName) {
			status = http.StatusConflict
		}

		http.Error(w, err.Error(), status)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("recipe created"))
}

func (h *RecipeHandler) Run(w http.ResponseWriter, r *http.Request) {
	var payload RecipeRunRequest
	err := decodeAndValidate(r.Body, "RunRecipeRequest", &payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	recipe, err := h.recipeService.Find(payload.RecipeName)
	if err != nil {
		status := http.StatusInternalServerError
		if _, ok := err.(domain.RecipeNotFoundError); ok {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}

	run, err := h.recipeService.Run(recipe)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(run)
}
