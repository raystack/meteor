package mocks

import (
	"github.com/odpf/meteor/domain"
	"github.com/stretchr/testify/mock"
)

type RecipeStore struct {
	mock.Mock
}

func (store *RecipeStore) GetByName(name string) (domain.Recipe, error) {
	args := store.Called(name)
	return args.Get(0).(domain.Recipe), args.Error(1)
}

func (store *RecipeStore) Create(recipe domain.Recipe) error {
	args := store.Called(recipe)
	return args.Error(0)
}
