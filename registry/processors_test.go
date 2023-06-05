package registry_test

import (
	"testing"

	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
)

func TestProcessorFactoryGet(t *testing.T) {
	t.Run("should return not found error if processor does not exist", func(t *testing.T) {
		name := "wrong-name"

		factory := registry.Processors
		if err := factory.Register("mock", newProcessor(mocks.NewProcessor())); err != nil {
			t.Error(err.Error())
		}
		_, err := factory.Get(name)
		assert.Equal(t, plugins.NotFoundError{Type: "processor", Name: name}, err)
	})

	t.Run("should return a new instance of processor with given name", func(t *testing.T) {
		name := "mock3"

		factory := registry.Processors
		if err := factory.Register(name, newProcessor(mocks.NewProcessor())); err != nil {
			t.Error(err.Error())
		}

		extr, err := factory.Get(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, mocks.NewProcessor(), extr)  // Same type
		assert.True(t, mocks.NewProcessor() != extr) // Different instance
	})
}

func TestProcessorFactoryRegister(t *testing.T) {
	t.Run("should add processor factory with given key", func(t *testing.T) {
		factory := registry.Processors
		err := factory.Register("mock1", newProcessor(mocks.NewProcessor()))
		if err != nil {
			t.Error(err.Error())
		}
		err = factory.Register("mock2", newProcessor(mocks.NewProcessor()))
		if err != nil {
			t.Error(err.Error())
		}

		mock1, err := factory.Get("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mocks.NewProcessor(), mock1)  // Same type
		assert.True(t, mocks.NewProcessor() != mock1) // Different instance

		mock2, err := factory.Get("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mocks.NewProcessor(), mock2)  // Same type
		assert.True(t, mocks.NewProcessor() != mock2) // Different instance

		err = factory.Register("mock1", newProcessor(mocks.NewProcessor())) // error for duplicate processor
		assert.Error(t, err)
	})
}

func TestProcessorFactoryList(t *testing.T) {
	t.Run("return list for a processor factory", func(t *testing.T) {
		factory := registry.NewProcessorFactory()
		extr := mocks.NewProcessor()
		mockInfo := plugins.Info{
			Description: "Mock Processor 1",
		}
		extr.On("Info").Return(mockInfo, nil).Once()
		defer extr.AssertExpectations(t)
		err := factory.Register("mock1", newProcessor(extr))
		if err != nil {
			t.Error(err.Error())
		}
		list := factory.List()
		assert.Equal(t, mockInfo, list["mock1"])
	})
}

func TestProcessorFactoryInfo(t *testing.T) {
	t.Run("return error for a processor not found", func(t *testing.T) {
		factory := registry.NewProcessorFactory()
		extr := mocks.NewProcessor()
		err := factory.Register("mock1", newProcessor(extr))
		if err != nil {
			t.Error(err.Error())
		}
		_, err = factory.Info("mock")
		assert.Equal(t, plugins.NotFoundError{Type: plugins.PluginTypeProcessor, Name: "mock"}, err)
	})
}

func newProcessor(extr plugins.Processor) func() plugins.Processor {
	return func() plugins.Processor {
		return extr
	}
}
