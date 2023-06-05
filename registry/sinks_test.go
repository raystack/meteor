package registry_test

import (
	"testing"

	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
)

func TestSinkFactoryGet(t *testing.T) {
	t.Run("should return not found error if sink does not exist", func(t *testing.T) {
		name := "wrong-name"

		factory := registry.Sinks
		if err := factory.Register("mock", newSink(mocks.NewSink())); err != nil {
			t.Error(err.Error())
		}
		_, err := factory.Get(name)
		assert.Equal(t, plugins.NotFoundError{Type: "sink", Name: name}, err)
	})

	t.Run("should return a new instance of sink with given name", func(t *testing.T) {
		name := "mock3"

		factory := registry.Sinks
		if err := factory.Register(name, newSink(mocks.NewSink())); err != nil {
			t.Error(err.Error())
		}

		extr, err := factory.Get(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, mocks.NewSink(), extr)  // Same type
		assert.True(t, mocks.NewSink() != extr) // Different instance
	})
}

func TestSinkFactoryRegister(t *testing.T) {
	t.Run("should add sink factory with given key", func(t *testing.T) {
		factory := registry.Sinks
		err := factory.Register("mock1", newSink(mocks.NewSink()))
		if err != nil {
			t.Error(err.Error())
		}
		err = factory.Register("mock2", newSink(mocks.NewSink()))
		if err != nil {
			t.Error(err.Error())
		}

		mock1, err := factory.Get("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mocks.NewSink(), mock1)  // Same type
		assert.True(t, mocks.NewSink() != mock1) // Different instance

		mock2, err := factory.Get("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mocks.NewSink(), mock2)  // Same type
		assert.True(t, mocks.NewSink() != mock2) // Different instance

		err = factory.Register("mock1", newSink(mocks.NewSink())) // error for duplicate sink
		assert.Error(t, err)
	})
}

func TestSinkFactoryList(t *testing.T) {
	t.Run("return list for a sink factory", func(t *testing.T) {
		factory := registry.NewSinkFactory()
		extr := mocks.NewSink()
		mockInfo := plugins.Info{
			Description: "Mock Sink 1",
		}
		extr.On("Info").Return(mockInfo, nil).Once()
		defer extr.AssertExpectations(t)
		err := factory.Register("mock1", newSink(extr))
		if err != nil {
			t.Error(err.Error())
		}
		list := factory.List()
		assert.Equal(t, mockInfo, list["mock1"])
	})
}

func TestSinkFactoryInfo(t *testing.T) {
	t.Run("return error for a sink not found", func(t *testing.T) {
		factory := registry.NewSinkFactory()
		extr := mocks.NewSink()
		err := factory.Register("mock1", newSink(extr))
		if err != nil {
			t.Error(err.Error())
		}
		_, err = factory.Info("mock")
		assert.Equal(t, plugins.NotFoundError{Type: plugins.PluginTypeSink, Name: "mock"}, err)
	})
}

func newSink(extr plugins.Syncer) func() plugins.Syncer {
	return func() plugins.Syncer {
		return extr
	}
}
