package metrics_test

import (
	"context"
	"testing"

	"github.com/raystack/meteor/agent"
	"github.com/raystack/meteor/config"
	"github.com/raystack/meteor/metrics"
	"github.com/raystack/meteor/recipe"
	"github.com/raystack/salt/log"
	"github.com/stretchr/testify/assert"
)

func TestOtelMonitor_RecordRun(t *testing.T) {
	ctx := context.Background()

	t.Run("should create metrics with the correct name and value", func(t *testing.T) {
		recipe := recipe.Recipe{
			Name: "test-recipe",
			Source: recipe.PluginRecipe{
				Name: "mysql",
			},
		}
		duration := 100
		recordCount := 2

		done, err := metrics.InitOtel(ctx, config.Config{
			OtelEnabled:       true,
			OtelCollectorAddr: "localhost:4317",
		}, log.NewLogrus(), "")
		defer done()
		assert.Nil(t, err)

		monitor, err := metrics.NewOtelMonitor()

		monitor.RecordRun(ctx, agent.Run{Recipe: recipe, DurationInMs: duration, RecordCount: recordCount, Success: false})

		assert.Nil(t, err)
		assert.NotNil(t, monitor)
		assert.NotNil(t, done)
	})
}

func TestOtelMonitor_RecordPlugin(t *testing.T) {
	ctx := context.Background()
	t.Run("should create metrics with the correct name and value", func(t *testing.T) {
		recipe := recipe.Recipe{
			Name: "test-recipe",
			Source: recipe.PluginRecipe{
				Name: "bigquery",
			},
			Sinks: []recipe.PluginRecipe{
				{Name: "test-sink"},
			},
		}

		done, err := metrics.InitOtel(ctx, config.Config{
			OtelEnabled:       true,
			OtelCollectorAddr: "localhost:4317",
		}, log.NewLogrus(), "")
		defer done()
		assert.Nil(t, err)

		monitor, err := metrics.NewOtelMonitor()

		monitor.RecordPlugin(context.Background(),
			agent.PluginInfo{
				RecipeName: recipe.Name,
				PluginName: recipe.Sinks[0].Name,
				PluginType: "sink",
				Success:    true,
			})
		assert.Nil(t, err)
		assert.NotNil(t, monitor)
		assert.NotNil(t, done)
	})
}
