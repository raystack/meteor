package processors_test

import (
	"testing"

	"github.com/odpf/meteor/processors"
	"github.com/stretchr/testify/assert"
)

type mockProcessor struct {
}

func (p *mockProcessor) Process(list []map[string]interface{}, config map[string]interface{}) ([]map[string]interface{}, error) {
	return list, nil
}

func TestStoreGet(t *testing.T) {
	t.Run("should return not found error if processor does not exist", func(t *testing.T) {
		name := "wrong-name"

		store := processors.NewStore()
		store.Set("mock", new(mockProcessor))

		_, err := store.Get(name)
		assert.Equal(t, processors.NotFoundError{name}, err)
	})

	t.Run("should return a processor with given name", func(t *testing.T) {
		name := "mock"
		mockProc := new(mockProcessor)

		store := processors.NewStore()
		store.Set(name, mockProc)

		actual, err := store.Get(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, mockProc, actual)
	})
}

func TestStoreSet(t *testing.T) {
	t.Run("should populate store with processors map", func(t *testing.T) {
		mock1 := new(mockProcessor)
		mock2 := new(mockProcessor)

		store := processors.NewStore()
		store.Set("mock1", new(mockProcessor))
		store.Set("mock2", new(mockProcessor))

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
