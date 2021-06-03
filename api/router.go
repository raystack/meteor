package api

import (
	"net/http"

	"github.com/gorilla/mux"
)

func NewRouter() *mux.Router {
	return mux.NewRouter()
}

func SetupRoutes(router *mux.Router) {
	router.Methods(http.MethodGet).Path("/").HandlerFunc(handleRoot)
	router.Methods(http.MethodGet).Path("/ping").HandlerFunc(handlePing)
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello"))
}

func handlePing(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("pong"))
}
