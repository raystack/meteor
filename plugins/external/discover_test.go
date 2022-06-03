// we are not using plugins_test package because
// we want to test isPlugin function which is a private function.
// TODO: change package name to "plugins_test"
package plugins

import (
	"testing"

	"github.com/odpf/meteor/registry"
	"github.com/stretchr/testify/assert"
)

func TestDiscoverPlugins(t *testing.T) {
	// TODO: add test
	factory := registry.NewProcessorFactory()
	_, err := DiscoverPlugins(factory)
	assert.Nil(t, err)
}

// once we already setup a test for DiscoverPlugins this test will not be needed
// TODO: remove test if TestDiscoverPlugins is already added
func TestIsPlugin(t *testing.T) {
	t.Run("should return true for correct format", func(t *testing.T) {
		files := []string{
			"meteor-plugin-test",
			"meteor-plugin-myplugin",
			"meteor-plugin-my_plugin",
			"meteor-plugin-a",
		}

		for _, fileName := range files {
			res := isPlugin(fileName)
			assert.True(t, res)
		}
	})

	t.Run("should return false for incorrect format", func(t *testing.T) {
		files := []string{
			"test",
			"meteor-plgin-test",
			"plugin-meteor-myplugin",
			"metor-plugin-my_plugin",
			"myplugin-meteor-plugin",
			"meteor-test-plugin",
			"meteor-test-",
			"meteor-test",
		}

		for _, fileName := range files {
			res := isPlugin(fileName)
			assert.False(t, res)
		}
	})
}
