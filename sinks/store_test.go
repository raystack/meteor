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

func TestStoreGet(t *testing.T) {
	t.Run("should return not found error if sink does not exist", func(t *testing.T) {
		name := "wrong-name"

		store := sinks.NewStore()
		store.Set("mock", new(mockSink))

		_, err := store.Get(name)
		assert.Equal(t, sinks.NotFoundError{name}, err)
	})

	t.Run("should return a sink with given name", func(t *testing.T) {
		name := "mock"
		mockProc := new(mockSink)

		store := sinks.NewStore()
		store.Set(name, mockProc)

		actual, err := store.Get(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, mockProc, actual)
	})
}

func TestStoreSet(t *testing.T) {
	t.Run("should populate store with sinks map", func(t *testing.T) {
		mock1 := new(mockSink)
		mock2 := new(mockSink)

		store := sinks.NewStore()
		store.Set("mock1", new(mockSink))
		store.Set("mock2", new(mockSink))

		mock1Actual, err := store.Get("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mock1, mock1Actual)

		mock2Actual, err := store.Get("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, mock2, mock2Actual)
	})
}
