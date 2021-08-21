package plugins

import (
	"encoding/json"
	"net/rpc"

	"github.com/hashicorp/go-plugin"
	"github.com/odpf/meteor/plugins"
)

// Processor is wrapper for processor.Processor
// it requires Name() to return the name of the processor
// it is needed for referencing it in a recipe
type Processor interface {
	plugins.Processor
	Name() (string, error)
}

type processorArgs struct {
	Data   interface{}
	Config map[string]interface{}
}

type ProcessorRPC struct {
	client *rpc.Client
}

// This function will be run on the host
func (e *ProcessorRPC) Name() (name string, err error) {
	err = e.client.Call("Plugin.Name", new(interface{}), &name)
	if err != nil {
		return
	}

	return
}

// This function will be run on the host
func (e *ProcessorRPC) Process(data interface{}, config map[string]interface{}) (resp []interface{}, err error) {
	args, err := json.Marshal(processorArgs{
		Data:   data,
		Config: config,
	})
	if err != nil {
		return resp, err
	}

	err = e.client.Call("Plugin.Process", args, &resp)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

type ProcessorRPCServer struct {
	// This is the real implementation
	Impl Processor
}

// This function will be run on the remote plugin
func (s *ProcessorRPCServer) Name(args interface{}, name *string) (err error) {
	*name, err = s.Impl.Name()
	if err != nil {
		return
	}

	return
}

// This function will be run on the remote plugin
func (s *ProcessorRPCServer) Process(argsBytes []byte, res *interface{}) (err error) {
	var args processorArgs
	err = json.Unmarshal(argsBytes, &args)
	if err != nil {
		return
	}

	//TODO: runtime processors are broken
	//*res, err = s.Impl.Process(args.Config)
	//if err != nil {
	//	return
	//}
	return
}

type ProcessorPlugin struct {
	// Impl Injection
	Impl Processor
}

func (p *ProcessorPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &ProcessorRPCServer{Impl: p.Impl}, nil
}

func (ProcessorPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &ProcessorRPC{client: c}, nil
}
