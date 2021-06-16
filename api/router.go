package api

import (
	"net/http"

	"github.com/gorilla/mux"
)

func NewRouter() *mux.Router {
	return mux.NewRouter()
}

func SetupRoutes(router *mux.Router, recipeHandler *RecipeHandler, secretHandler *SecretHandler) {
	router.Methods(http.MethodGet).Path("/").HandlerFunc(handleRoot)
	router.Methods(http.MethodGet).Path("/ping").HandlerFunc(handlePing)

	router.Methods(http.MethodPost).Path("/v1/recipes").HandlerFunc(recipeHandler.Create)
	router.Methods(http.MethodPost).Path("/v1/run").HandlerFunc(recipeHandler.Run)
	router.Methods(http.MethodPut).Path("/v1/secrets").HandlerFunc(secretHandler.Upsert)
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello"))
}

func handlePing(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("pong"))
}
