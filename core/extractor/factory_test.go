package extractor_test

import (
	"testing"

	"github.com/odpf/meteor/core/extractor"
	"github.com/stretchr/testify/assert"
)

type mockExtractor struct {
}

func (p *mockExtractor) Extract(config map[string]interface{}) ([]map[string]interface{}, error) {
	return []map[string]interface{}{}, nil
}

func newMockExtractor() extractor.Extractor {
	return &mockExtractor{}
}

func TestFactoryGet(t *testing.T) {
	t.Run("should return not found error if extractor does not exist", func(t *testing.T) {
		name := "wrong-name"

		factory := extractor.NewFactory()
		factory.Set("mock", newMockExtractor)

		_, err := factory.Get(name)
		assert.Equal(t, extractor.NotFoundError{name}, err)
	})

	t.Run("should return a new instance of extractor with given name", func(t *testing.T) {
		name := "mock"

		factory := extractor.NewFactory()
		factory.Set(name, newMockExtractor)

		extr, err := factory.Get(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, new(mockExtractor), extr)  // Same type
		assert.True(t, new(mockExtractor) != extr) // Different instance
	})
}

func TestFactorySet(t *testing.T) {
	t.Run("should add extractor factory with given key", func(t *testing.T) {
		factory := extractor.NewFactory()
		factory.Set("mock1", newMockExtractor)
		factory.Set("mock2", newMockExtractor)

		mock1, err := factory.Get("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockExtractor), mock1)  // Same type
		assert.True(t, new(mockExtractor) != mock1) // Different instance

		mock2, err := factory.Get("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockExtractor), mock2)  // Same type
		assert.True(t, new(mockExtractor) != mock2) // Different instance
	})
}
