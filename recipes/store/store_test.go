package store_test

import (
	"testing"

	"github.com/odpf/meteor/domain"
	"github.com/odpf/meteor/recipes"
	"github.com/odpf/meteor/recipes/store"
	"github.com/stretchr/testify/assert"
)

func TestStoreGetByName(t *testing.T) {
	t.Run("should return error if recipe could not be found", func(t *testing.T) {
		store, err := store.New("mem://")
		if err != nil {
			t.Error(err)
		}
		err = store.Create(domain.Recipe{
			Name: "sample",
		})
		if err != nil {
			t.Error(err)
		}

		_, err = store.GetByName("wrong-name")
		assert.NotNil(t, err)
		assert.Equal(t, recipes.NotFoundError{RecipeName: "wrong-name"}, err)
	})

	t.Run("should return recipe with the given name", func(t *testing.T) {
		name := "sample"

		store, err := store.New("mem://")
		if err != nil {
			t.Error(err)
		}
		err = store.Create(domain.Recipe{
			Name: "sample",
		})
		if err != nil {
			t.Error(err)
		}

		recp, err := store.GetByName(name)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, name, recp.Name)
	})
}

func TestStoreCreate(t *testing.T) {
	t.Run("should persist recipe", func(t *testing.T) {
		recp := domain.Recipe{
			Name: "sample",
		}

		store, err := store.New("mem://")
		if err != nil {
			t.Error(err)
		}
		err = store.Create(recp)
		if err != nil {
			t.Error(err)
		}

		result, err := store.GetByName(recp.Name)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, recp, result)
	})
}
