package plugins

import (
	"errors"
	"os/exec"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
)

var (
	handshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  2,
		MagicCookieKey:   "METEOR_PLUGIN",
		MagicCookieValue: "F$i^yqI.s]NIoHhR'fVV{=@ix-:gyN",
	}
	processorPluginKey = "processor"
)

func ServeProcessor(processor Processor, logger hclog.Logger) {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins: map[string]plugin.Plugin{
			processorPluginKey: &ProcessorPlugin{
				Impl: processor,
			},
		},
		Logger: logger,
	})
}

func NewClient(binaryPath string) *plugin.Client {
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins: map[string]plugin.Plugin{
			processorPluginKey: &ProcessorPlugin{},
		},
		Cmd: exec.Command(binaryPath),
		Logger: hclog.New(&hclog.LoggerOptions{
			Level: hclog.Debug, // Log level Debug is the minimum to log error from plugin
		}),
	})

	return client
}

func dispense(client *plugin.Client) (processor Processor, err error) {
	// Connect via RPC
	rpcClient, err := client.Client()
	if err != nil {
		return
	}
	// Request the plugin
	raw, err := rpcClient.Dispense(processorPluginKey)
	if err != nil {
		return
	}

	processor, ok := raw.(Processor)
	if !ok {
		return processor, errors.New("invalid processor format")
	}

	return
}
