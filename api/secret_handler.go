package api

import (
	"net/http"

	"github.com/odpf/meteor/secrets"
)

type SecretRunRequest struct {
	SecretName string `json:"secret_name"`
}

type SecretHandler struct {
	secretStore secrets.Store
}

func NewSecretHandler(secretStore secrets.Store) *SecretHandler {
	return &SecretHandler{
		secretStore: secretStore,
	}
}

func (h *SecretHandler) Upsert(w http.ResponseWriter, r *http.Request) {
	var secret secrets.Secret
	err := decodeAndValidate(r.Body, "UpsertSecretRequest", &secret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.secretStore.Upsert(secret); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("secret updated"))
}
