package processor_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/core/processor"
	"github.com/stretchr/testify/assert"
)

type mockProcessor struct {
}

func (p *mockProcessor) Process(ctx context.Context, config map[string]interface{}, in <-chan interface{}, out chan<- interface{}) (err error) {
	out <- in
	return nil
}

func newMockProcessor() *mockProcessor {
	return new(mockProcessor)
}

func TestFactoryGet(t *testing.T) {
	t.Run("should return not found error if processor does not exist", func(t *testing.T) {
		name := "wrong-name"

		factory := processor.NewFactory()
		factory.Register("mock", newMockProcessor())

		_, err := factory.Get(name)
		assert.Equal(t, processor.NotFoundError{name}, err)
	})

	t.Run("should return a new instance of processor with given name", func(t *testing.T) {
		name := "mock"

		factory := processor.NewFactory()
		factory.Register(name, newMockProcessor())

		extr, err := factory.Get(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, new(mockProcessor), extr)  // Same type
		assert.True(t, new(mockProcessor) != extr) // Different instance
	})
}

func TestFactoryRegister(t *testing.T) {
	t.Run("should add processor factory with given key", func(t *testing.T) {
		factory := processor.NewFactory()
		factory.Register("mock1", newMockProcessor())
		factory.Register("mock2", newMockProcessor())

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
