package registry_test

import (
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
)

func TestFactoryGet(t *testing.T) {
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

func TestFactoryRegister(t *testing.T) {
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
	})
}

func newExtractor(extr plugins.Extractor) func() plugins.Extractor {
	return func() plugins.Extractor {
		return extr
	}
}
