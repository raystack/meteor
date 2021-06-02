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

func TestStoreFind(t *testing.T) {
	t.Run("should return not found error if processor does not exist", func(t *testing.T) {
		name := "wrong-name"

		store := processors.NewStore()
		store.Populate(map[string]processors.Processor{
			"mock": new(mockProcessor),
		})

		_, err := store.Find(name)
		assert.Equal(t, processors.NotFoundError{name}, err)
	})

	t.Run("should return a processor with given name", func(t *testing.T) {
		name := "mock"
		mockProc := new(mockProcessor)

		store := processors.NewStore()
		store.Populate(map[string]processors.Processor{
			name: mockProc,
		})

		actual, err := store.Find(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, mockProc, actual)
	})
}

func TestStorePopulate(t *testing.T) {
	t.Run("should populate store with processors map", func(t *testing.T) {
		mock1 := new(mockProcessor)
		mock2 := new(mockProcessor)

		store := processors.NewStore()
		store.Populate(map[string]processors.Processor{
			"mock1": new(mockProcessor),
			"mock2": new(mockProcessor),
		})

		mock1Actual, err := store.Find("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mock1, mock1Actual)

		mock2Actual, err := store.Find("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mock2, mock2Actual)
	})

	t.Run("should add processors to existing map", func(t *testing.T) {
		mock1 := new(mockProcessor)
		mock2 := new(mockProcessor)

		store := processors.NewStore()
		store.Populate(map[string]processors.Processor{
			"mock1": new(mockProcessor),
		})

		store.Populate(map[string]processors.Processor{
			"mock2": new(mockProcessor),
		})

		mock1Actual, err := store.Find("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mock1, mock1Actual)

		mock2Actual, err := store.Find("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mock2, mock2Actual)
	})
}
