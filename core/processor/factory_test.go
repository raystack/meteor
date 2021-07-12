package processor_test

import (
	"testing"

	"github.com/odpf/meteor/core/processor"
	"github.com/stretchr/testify/assert"
)

type mockProcessor struct {
}

func (p *mockProcessor) Process(data []map[string]interface{}, config map[string]interface{}) ([]map[string]interface{}, error) {
	return data, nil
}

func newMockProcessor() processor.Processor {
	return new(mockProcessor)
}

func TestFactoryGet(t *testing.T) {
	t.Run("should return not found error if processor does not exist", func(t *testing.T) {
		name := "wrong-name"

		factory := processor.NewFactory()
		factory.Set("mock", newMockProcessor)

		_, err := factory.Get(name)
		assert.Equal(t, processor.NotFoundError{name}, err)
	})

	t.Run("should return a new instance of processor with given name", func(t *testing.T) {
		name := "mock"

		factory := processor.NewFactory()
		factory.Set(name, newMockProcessor)

		extr, err := factory.Get(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, new(mockProcessor), extr)  // Same type
		assert.True(t, new(mockProcessor) != extr) // Different instance
	})
}

func TestFactorySet(t *testing.T) {
	t.Run("should add processor factory with given key", func(t *testing.T) {
		factory := processor.NewFactory()
		factory.Set("mock1", newMockProcessor)
		factory.Set("mock2", newMockProcessor)

		mock1, err := factory.Get("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockProcessor), mock1)  // Same type
		assert.True(t, new(mockProcessor) != mock1) // Different instance

		mock2, err := factory.Get("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockProcessor), mock2)  // Same type
		assert.True(t, new(mockProcessor) != mock2) // Different instance
	})
}
