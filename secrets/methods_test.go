package secrets_test

import (
	"errors"
	"testing"

	"github.com/odpf/meteor/mocks"
	"github.com/odpf/meteor/secrets"
	"github.com/stretchr/testify/assert"
)

func TestMapConfig(t *testing.T) {
	t.Run("should not do anything if config does not reference secret", func(t *testing.T) {
		config := map[string]interface{}{
			"username": "john",
			"password": "foobar",
		}

		store := new(mocks.SecretStore)
		err := secrets.MapConfig(config, store)
		if err != nil {
			t.Fatal(err.Error())
		}

		expected := map[string]interface{}{
			"username": "john",
			"password": "foobar",
		}
		assert.Equal(t, expected, config)
	})

	t.Run("should map config value to secret if secret is referenced", func(t *testing.T) {
		config := map[string]interface{}{
			"id":   "$secret.admin.username",
			"pwd":  "$secret.admin.password",
			"host": "localhost:3000",
		}
		secret := secrets.Secret{
			Name: "admin",
			Data: map[string]interface{}{
				"username": "john",
				"password": "foobar",
			},
		}

		store := new(mocks.SecretStore)
		store.On("Find", secret.Name).Return(secret, nil)
		defer store.AssertExpectations(t)

		err := secrets.MapConfig(config, store)
		if err != nil {
			t.Fatal(err.Error())
		}

		expected := map[string]interface{}{
			"id":   "john",
			"pwd":  "foobar",
			"host": "localhost:3000",
		}
		assert.Equal(t, expected, config)
	})

	t.Run("should return error if secret could not be found", func(t *testing.T) {
		config := map[string]interface{}{
			"id": "$secret.admin.username",
		}
		expectedErr := errors.New("could not find secret")

		store := new(mocks.SecretStore)
		store.On("Find", "admin").Return(secrets.Secret{}, expectedErr)
		defer store.AssertExpectations(t)

		err := secrets.MapConfig(config, store)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("should return error if secret's key could not be found", func(t *testing.T) {
		config := map[string]interface{}{
			"id": "$secret.admin.username",
		}
		secret := secrets.Secret{
			Name: "admin",
			Data: map[string]interface{}{},
		}

		store := new(mocks.SecretStore)
		store.On("Find", secret.Name).Return(secret, nil)
		defer store.AssertExpectations(t)

		err := secrets.MapConfig(config, store)
		assert.NotNil(t, err)
	})
}
