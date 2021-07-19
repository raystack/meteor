package sink_test

import (
	"testing"

	"github.com/odpf/meteor/core/sink"
	"github.com/stretchr/testify/assert"
)

type mockSink struct {
}

func (p *mockSink) Sink(data interface{}, config map[string]interface{}) error {
	return nil
}

func newMockSink() sink.Sink {
	return &mockSink{}
}

func TestFactoryGet(t *testing.T) {
	t.Run("should return not found error if sink does not exist", func(t *testing.T) {
		name := "wrong-name"

		factory := sink.NewFactory()
		factory.Set("mock", newMockSink)

		_, err := factory.Get(name)
		assert.Equal(t, sink.NotFoundError{name}, err)
	})

	t.Run("should return a new instance of sink with given name", func(t *testing.T) {
		name := "mock"

		factory := sink.NewFactory()
		factory.Set(name, newMockSink)

		extr, err := factory.Get(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, new(mockSink), extr)  // Same type
		assert.True(t, new(mockSink) != extr) // Different instance
	})
}

func TestFactorySet(t *testing.T) {
	t.Run("should add sink factory with given key", func(t *testing.T) {
		factory := sink.NewFactory()
		factory.Set("mock1", newMockSink)
		factory.Set("mock2", newMockSink)

		mock1, err := factory.Get("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockSink), mock1)  // Same type
		assert.True(t, new(mockSink) != mock1) // Different instance

		mock2, err := factory.Get("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockSink), mock2)  // Same type
		assert.True(t, new(mockSink) != mock2) // Different instance
	})
}
