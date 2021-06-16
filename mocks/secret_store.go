package mocks

import (
	"github.com/odpf/meteor/secrets"
	"github.com/stretchr/testify/mock"
)

type SecretStore struct {
	mock.Mock
}

func (store *SecretStore) Find(name string) (secrets.Secret, error) {
	args := store.Called(name)
	return args.Get(0).(secrets.Secret), args.Error(1)
}

func (store *SecretStore) Upsert(secret secrets.Secret) error {
	args := store.Called(secret)
	return args.Error(0)
}
