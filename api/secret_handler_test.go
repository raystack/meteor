package api_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/odpf/meteor/api"
	"github.com/odpf/meteor/mocks"
	"github.com/odpf/meteor/secrets"
	"github.com/stretchr/testify/assert"
)

func TestSecretHandlerUpsert(t *testing.T) {
	t.Run("should return 404 on invalid payload", func(t *testing.T) {
		payload := `{
			"name": "bar",
			"data": null
		}`

		secretStore := new(mocks.SecretStore)

		handler := api.NewSecretHandler(secretStore)
		rr := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
		rw := httptest.NewRecorder()
		handler.Upsert(rw, rr)

		assert.Equal(t, http.StatusBadRequest, rw.Result().StatusCode)
	})

	t.Run("should return 500 when store returns error", func(t *testing.T) {
		payload := `{
			"name": "sample",
			"data": {
				"foo": "bar"
			}
		}`
		secret := secrets.Secret{
			Name: "sample",
			Data: map[string]interface{}{
				"foo": "bar",
			},
		}

		secretStore := new(mocks.SecretStore)
		secretStore.On("Upsert", secret).Return(errors.New("sample-error"))
		defer secretStore.AssertExpectations(t)

		handler := api.NewSecretHandler(secretStore)
		rr := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
		rw := httptest.NewRecorder()
		handler.Upsert(rw, rr)

		assert.Equal(t, http.StatusInternalServerError, rw.Result().StatusCode)
	})

	t.Run("should return 200 on success", func(t *testing.T) {
		payload := `{
			"name": "sample",
			"data": {
				"foo": "bar"
			}
		}`
		secret := secrets.Secret{
			Name: "sample",
			Data: map[string]interface{}{
				"foo": "bar",
			},
		}

		secretStore := new(mocks.SecretStore)
		secretStore.On("Upsert", secret).Return(nil)
		defer secretStore.AssertExpectations(t)

		handler := api.NewSecretHandler(secretStore)
		rr := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
		rw := httptest.NewRecorder()
		handler.Upsert(rw, rr)

		assert.Equal(t, http.StatusOK, rw.Result().StatusCode)
	})
}
