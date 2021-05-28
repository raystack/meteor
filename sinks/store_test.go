package sinks_test

import (
	"testing"

	"github.com/odpf/meteor/sinks"
	"github.com/stretchr/testify/assert"
)

type mockSink struct {
}

func (p *mockSink) Sink(data []map[string]interface{}, config map[string]interface{}) error {
	return nil
}

func TestStoreFind(t *testing.T) {
	t.Run("should return not found error if sink does not exist", func(t *testing.T) {
		name := "wrong-name"

		store := sinks.NewStore()
		store.Populate(map[string]sinks.Sink{
			"mock": new(mockSink),
		})

		_, err := store.Find(name)
		assert.Equal(t, sinks.NotFoundError{name}, err)
	})

	t.Run("should return a sink with given name", func(t *testing.T) {
		name := "mock"
		mockProc := new(mockSink)

		store := sinks.NewStore()
		store.Populate(map[string]sinks.Sink{
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
	t.Run("should populate store with sinks map", func(t *testing.T) {
		mock1 := new(mockSink)
		mock2 := new(mockSink)

		store := sinks.NewStore()
		store.Populate(map[string]sinks.Sink{
			"mock1": new(mockSink),
			"mock2": new(mockSink),
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

	t.Run("should add sinks to existing map", func(t *testing.T) {
		mock1 := new(mockSink)
		mock2 := new(mockSink)

		store := sinks.NewStore()
		store.Populate(map[string]sinks.Sink{
			"mock1": new(mockSink),
		})

		store.Populate(map[string]sinks.Sink{
			"mock2": new(mockSink),
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
