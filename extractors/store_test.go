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

func TestStoreFind(t *testing.T) {
	t.Run("should return not found error if extractor does not exist", func(t *testing.T) {
		name := "wrong-name"

		store := extractors.NewStore()
		store.Populate(map[string]extractors.Extractor{
			"mock": new(mockExtractor),
		})

		_, err := store.Find(name)
		assert.Equal(t, extractors.NotFoundError{name}, err)
	})

	t.Run("should return a extractor with given name", func(t *testing.T) {
		name := "mock"
		mockProc := new(mockExtractor)

		store := extractors.NewStore()
		store.Populate(map[string]extractors.Extractor{
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
	t.Run("should populate store with extractors map", func(t *testing.T) {
		mock1 := new(mockExtractor)
		mock2 := new(mockExtractor)

		store := extractors.NewStore()
		store.Populate(map[string]extractors.Extractor{
			"mock1": new(mockExtractor),
			"mock2": new(mockExtractor),
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

	t.Run("should add extractors to existing map", func(t *testing.T) {
		mock1 := new(mockExtractor)
		mock2 := new(mockExtractor)

		store := extractors.NewStore()
		store.Populate(map[string]extractors.Extractor{
			"mock1": new(mockExtractor),
		})

		store.Populate(map[string]extractors.Extractor{
			"mock2": new(mockExtractor),
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
