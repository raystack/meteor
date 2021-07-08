package plugins

import (
	"os"

	"github.com/hashicorp/go-plugin"
	"github.com/odpf/meteor/processors"
)

var (
	pluginPrefix = "meteor-plugin-"
)

// This functions discovers plugins and populate processors with them
// returns clean up function to kill plugins processes
//
// discover plugins from
// ./
// with the following format meteor-plugin-{plugin_name}
//
// in case of duplicate processor name, the latest would be used with no guarantee in order
func DiscoverPlugins(factory *processors.Factory) (killPluginsFn func(), err error) {
	binaries, err := findBinaries()
	if err != nil {
		return
	}
	clients, err := createClients(binaries)
	if err != nil {
		return
	}
	killPluginsFn = buildKillPluginsFn(clients)

	err = populateFactory(clients, factory)
	if err != nil {
		killPluginsFn() // kill plugins processes to prevent hanging processes
		return
	}

	return
}

func findBinaries() (binaries []string, err error) {
	path, err := os.Getwd() // current working directory
	if err != nil {
		return
	}
	dirEntries, err := os.ReadDir(path)
	if err != nil {
		return
	}
	for _, dirEntry := range dirEntries {
		if isPlugin(dirEntry.Name()) {
			binaries = append(binaries, "./"+dirEntry.Name())
		}
	}

	return
}
func createClients(binaries []string) (clients []*plugin.Client, err error) {
	for _, binary := range binaries {
		clients = append(clients, NewClient(binary))
	}
	return
}
func populateFactory(clients []*plugin.Client, factory *processors.Factory) (err error) {
	for _, client := range clients {
		processor, err := dispense(client)
		if err != nil {
			return err
		}
		name, err := processor.Name()
		if err != nil {
			return err
		}

		factory.Set(name, func() processors.Processor {
			return processor
		})
	}
	return
}
func isPlugin(filename string) bool {
	pluginPrefixLen := len(pluginPrefix)
	if len(filename) <= pluginPrefixLen {
		return false
	}

	return filename[:pluginPrefixLen] == pluginPrefix
}
func buildKillPluginsFn(clients []*plugin.Client) func() {
	return func() {
		for _, client := range clients {
			client.Kill()
		}
	}
}
