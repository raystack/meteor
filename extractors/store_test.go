package extractors_test

import (
	"testing"

	"github.com/odpf/meteor/extractors"
	"github.com/stretchr/testify/assert"
)

type mockExtractor struct {
}

func (p *mockExtractor) Extract(config map[string]interface{}) ([]map[string]interface{}, error) {
	return []map[string]interface{}{}, nil
}

func TestStoreGet(t *testing.T) {
	t.Run("should return not found error if extractor does not exist", func(t *testing.T) {
		name := "wrong-name"

		store := extractors.NewStore()
		store.Set("mock", new(mockExtractor))

		_, err := store.Get(name)
		assert.Equal(t, extractors.NotFoundError{name}, err)
	})

	t.Run("should return a extractor with given name", func(t *testing.T) {
		name := "mock"
		mockProc := new(mockExtractor)

		store := extractors.NewStore()
		store.Set(name, mockProc)

		actual, err := store.Get(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, mockProc, actual)
	})
}

func TestStoreSet(t *testing.T) {
	t.Run("should add extractor with given key", func(t *testing.T) {
		mock1 := new(mockExtractor)
		mock2 := new(mockExtractor)

		store := extractors.NewStore()
		store.Set("mock1", new(mockExtractor))
		store.Set("mock2", new(mockExtractor))

		mock1Actual, err := store.Get("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mock1, mock1Actual)

		mock2Actual, err := store.Get("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mock2, mock2Actual)
	})
}
