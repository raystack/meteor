package registry_test

import (
	"testing"

	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
)

func TestExtractorFactoryGet(t *testing.T) {
	t.Run("should return not found error if extractor does not exist", func(t *testing.T) {
		name := "wrong-name"

		factory := registry.Extractors
		if err := factory.Register("mock", newExtractor(mocks.NewExtractor())); err != nil {
			t.Error(err.Error())
		}
		_, err := factory.Get(name)
		assert.Equal(t, plugins.NotFoundError{Type: "extractor", Name: name}, err)
	})

	t.Run("should return a new instance of extractor with given name", func(t *testing.T) {
		name := "mock3"

		factory := registry.Extractors
		if err := factory.Register(name, newExtractor(mocks.NewExtractor())); err != nil {
			t.Error(err.Error())
		}

		extr, err := factory.Get(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, mocks.NewExtractor(), extr)  // Same type
		assert.True(t, mocks.NewExtractor() != extr) // Different instance
	})
}

func TestExtractorFactoryRegister(t *testing.T) {
	t.Run("should add extractor factory with given key", func(t *testing.T) {
		factory := registry.Extractors
		err := factory.Register("mock1", newExtractor(mocks.NewExtractor()))
		if err != nil {
			t.Error(err.Error())
		}
		err = factory.Register("mock2", newExtractor(mocks.NewExtractor()))
		if err != nil {
			t.Error(err.Error())
		}

		mock1, err := factory.Get("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mocks.NewExtractor(), mock1)  // Same type
		assert.True(t, mocks.NewExtractor() != mock1) // Different instance

		mock2, err := factory.Get("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mocks.NewExtractor(), mock2)  // Same type
		assert.True(t, mocks.NewExtractor() != mock2) // Different instance

		err = factory.Register("mock1", newExtractor(mocks.NewExtractor())) //error for duplicate extractor
		assert.Error(t, err)
	})
}

func TestExtractorFactoryList(t *testing.T) {
	t.Run("return list for a extractor factory", func(t *testing.T) {
		factory := registry.NewExtractorFactory()
		extr := mocks.NewExtractor()
		mockInfo := plugins.Info{
			Description: "Mock Extractor 1",
		}
		extr.On("Info").Return(mockInfo, nil).Once()
		defer extr.AssertExpectations(t)
		err := factory.Register("mock1", newExtractor(extr))
		if err != nil {
			t.Error(err.Error())
		}
		list := factory.List()
		assert.Equal(t, mockInfo, list["mock1"])
	})
}

func TestExtractorFactoryInfo(t *testing.T) {
	t.Run("return error for a extractor not found", func(t *testing.T) {
		factory := registry.NewExtractorFactory()
		extr := mocks.NewExtractor()
		err := factory.Register("mock1", newExtractor(extr))
		if err != nil {
			t.Error(err.Error())
		}
		_, err = factory.Info("mock")
		assert.Equal(t, plugins.NotFoundError{Type: plugins.PluginTypeExtractor, Name: "mock"}, err)
	})
}

func newExtractor(extr plugins.Extractor) func() plugins.Extractor {
	return func() plugins.Extractor {
		return extr
	}
}
