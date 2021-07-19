package extractor_test

import (
	"testing"

	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/stretchr/testify/assert"
)

func TestFactoryGet(t *testing.T) {
	t.Run("should return not found error if extractor does not exist", func(t *testing.T) {
		name := "wrong-name"

		factory := extractor.NewFactory()
		factory.SetTableExtractor("mock", newMockTableExtractor)

		_, err := factory.Get(name)
		assert.Equal(t, extractor.NotFoundError{name}, err)
	})

	t.Run("should return a new instance of extractor with given name", func(t *testing.T) {
		name := "mock"

		factory := extractor.NewFactory()
		factory.SetTableExtractor(name, newMockTableExtractor)

		extr, err := factory.Get(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, new(mockTableExtractor), extr)  // Same type
		assert.True(t, new(mockTableExtractor) != extr) // Different instance
	})
}

func TestFactorySetTableExtractor(t *testing.T) {
	t.Run("should add extractor factory with given key", func(t *testing.T) {
		factory := extractor.NewFactory()
		factory.SetTableExtractor("mock1", newMockTableExtractor)
		factory.SetTableExtractor("mock2", newMockTableExtractor)

		mock1, err := factory.Get("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockTableExtractor), mock1)  // Same type
		assert.True(t, new(mockTableExtractor) != mock1) // Different instance

		mock2, err := factory.Get("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockTableExtractor), mock2)  // Same type
		assert.True(t, new(mockTableExtractor) != mock2) // Different instance
	})
}

func TestFactorySetTopicExtractor(t *testing.T) {
	t.Run("should add extractor factory with given key", func(t *testing.T) {
		factory := extractor.NewFactory()
		factory.SetTopicExtractor("mock1", newMockTopicExtractor)
		factory.SetTopicExtractor("mock2", newMockTopicExtractor)

		mock1, err := factory.Get("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockTopicExtractor), mock1)  // Same type
		assert.True(t, new(mockTopicExtractor) != mock1) // Different instance

		mock2, err := factory.Get("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockTopicExtractor), mock2)  // Same type
		assert.True(t, new(mockTopicExtractor) != mock2) // Different instance
	})
}

func TestFactorySetDashboardExtractor(t *testing.T) {
	t.Run("should add extractor factory with given key", func(t *testing.T) {
		factory := extractor.NewFactory()
		factory.SetDashboardExtractor("mock1", newMockDashboardExtractor)
		factory.SetDashboardExtractor("mock2", newMockDashboardExtractor)

		mock1, err := factory.Get("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockDashboardExtractor), mock1)  // Same type
		assert.True(t, new(mockDashboardExtractor) != mock1) // Different instance

		mock2, err := factory.Get("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockDashboardExtractor), mock2)  // Same type
		assert.True(t, new(mockDashboardExtractor) != mock2) // Different instance
	})
}

type mockTableExtractor struct {
}

func (e *mockTableExtractor) Extract(config map[string]interface{}) ([]meta.Table, error) {
	return []meta.Table{}, nil
}

func newMockTableExtractor() extractor.TableExtractor {
	return &mockTableExtractor{}
}

type mockTopicExtractor struct {
}

func (e *mockTopicExtractor) Extract(config map[string]interface{}) ([]meta.Topic, error) {
	return []meta.Topic{}, nil
}

func newMockTopicExtractor() extractor.TopicExtractor {
	return &mockTopicExtractor{}
}

type mockDashboardExtractor struct {
}

func (e *mockDashboardExtractor) Extract(config map[string]interface{}) ([]meta.Dashboard, error) {
	return []meta.Dashboard{}, nil
}

func newMockDashboardExtractor() extractor.DashboardExtractor {
	return &mockDashboardExtractor{}
}
