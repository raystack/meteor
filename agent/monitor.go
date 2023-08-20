package agent

import (
	"context"
)

type PluginInfo struct {
	RecipeName string
	PluginName string
	PluginType string
	Success    bool
	BatchSize  int
}

// Monitor is the interface for monitoring the agent.
type Monitor interface {
	RecordRun(ctx context.Context, run Run)
	RecordPlugin(ctx context.Context, pluginInfo PluginInfo)
	RecordSinkRetryCount(ctx context.Context, pluginInfo PluginInfo)
}
