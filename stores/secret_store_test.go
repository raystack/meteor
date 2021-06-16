package stores_test

import (
	"testing"

	"github.com/odpf/meteor/secrets"
	"github.com/odpf/meteor/stores"
	"github.com/stretchr/testify/assert"
)

func TestStoreFind(t *testing.T) {
	t.Run("should return error if secret could not be found", func(t *testing.T) {
		store, err := stores.NewSecretStore("mem://")
		if err != nil {
			t.Error(err)
		}
		err = store.Upsert(secrets.Secret{
			Name: "sample",
		})
		if err != nil {
			t.Error(err)
		}

		_, err = store.Find("wrong-name")
		assert.NotNil(t, err)
		assert.Equal(t, secrets.NotFoundError{SecretName: "wrong-name"}, err)
	})

	t.Run("should return secret with the given name", func(t *testing.T) {
		name := "sample"

		store, err := stores.NewSecretStore("mem://")
		if err != nil {
			t.Error(err)
		}
		err = store.Upsert(secrets.Secret{
			Name: "sample",
		})
		if err != nil {
			t.Error(err)
		}

		recp, err := store.Find(name)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, name, recp.Name)
	})
}

func TestStoreUpsert(t *testing.T) {
	t.Run("should store new secret", func(t *testing.T) {
		secret := secrets.Secret{
			Name: "sample",
			Data: map[string]interface{}{
				"username": "John",
			},
		}

		store, err := stores.NewSecretStore("mem://")
		if err != nil {
			t.Error(err)
		}
		err = store.Upsert(secret)
		if err != nil {
			t.Error(err)
		}

		result, err := store.Find(secret.Name)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, secret, result)
	})

	t.Run("should replace existing secret", func(t *testing.T) {
		secretName := "sample"
		existingSecret := secrets.Secret{
			Name: secretName,
			Data: map[string]interface{}{},
		}
		newSecret := secrets.Secret{
			Name: secretName,
			Data: map[string]interface{}{
				"username": "John",
			},
		}

		store, err := stores.NewSecretStore("mem://")
		if err != nil {
			t.Fatal(err)
		}
		err = store.Upsert(existingSecret)
		if err != nil {
			t.Fatal(err)
		}
		err = store.Upsert(newSecret)
		if err != nil {
			t.Fatal(err)
		}

		result, err := store.Find(secretName)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, "John", result.Data["username"])
	})
}
