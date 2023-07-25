package metrics_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/goto/meteor/agent"
	"github.com/goto/meteor/metrics"
	"github.com/goto/meteor/recipe"
	"github.com/goto/meteor/test/utils"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockStatsdClient struct {
	mock.Mock
}

func (c *mockStatsdClient) Timing(name string, val int64) {
	c.Called(name, val)
}

func (c *mockStatsdClient) IncrementByValue(name string, val int) {
	c.Called(name, val)
}

func (c *mockStatsdClient) Increment(name string) {
	c.Called(name)
}

var port = "8125"

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "statsd/statsd",
		Platform:   "linux/amd64",
		Tag:        "latest",
		Env: []string{
			"MYSQL_ALLOW_EMPTY_PASSWORD=true",
		},
		ExposedPorts: []string{"8125", port},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8125": {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) error {
		c, err := metrics.NewStatsdClient("127.0.0.1:" + port)
		if err != nil {
			return err
		}
		c.Open()
		return nil
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	// clean tests
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestStatsdMonitorRecordRun(t *testing.T) {
	statsdPrefix := "testprefix"

	t.Run("should create metrics with the correct name and value", func(t *testing.T) {
		recipe := recipe.Recipe{
			Name: "test-recipe",
			Source: recipe.PluginRecipe{
				Name: "mysql",
			},
		}
		duration := 100
		recordCount := 2
		timingMetric := fmt.Sprintf(
			"%s.runDuration,name=%s,success=%s,extractor=%s",
			statsdPrefix,
			recipe.Name,
			"false",
			recipe.Source.Name,
		)
		incrementMetric := fmt.Sprintf(
			"%s.run,name=%s,success=%s,extractor=%s",
			statsdPrefix,
			recipe.Name,
			"false",
			recipe.Source.Name,
		)
		recordIncrementMetric := fmt.Sprintf(
			"%s.runRecordCount,name=%s,success=%s,extractor=%s",
			statsdPrefix,
			recipe.Name,
			"false",
			recipe.Source.Name,
		)

		client := new(mockStatsdClient)
		client.On("Timing", timingMetric, int64(duration))
		client.On("Increment", incrementMetric)
		client.On("IncrementByValue", recordIncrementMetric, recordCount)
		defer client.AssertExpectations(t)

		monitor := metrics.NewStatsdMonitor(client, statsdPrefix)
		monitor.RecordRun(context.Background(), agent.Run{Recipe: recipe, DurationInMs: duration, RecordCount: recordCount, Success: false})
	})

	t.Run("should set success field to true on success", func(t *testing.T) {
		recipe := recipe.Recipe{
			Name: "test-recipe",
			Source: recipe.PluginRecipe{
				Name: "bigquery",
			},
		}
		duration := 100
		recordCount := 2
		timingMetric := fmt.Sprintf(
			"%s.runDuration,name=%s,success=%s,extractor=%s",
			statsdPrefix,
			recipe.Name,
			"true",
			recipe.Source.Name,
		)
		incrementMetric := fmt.Sprintf(
			"%s.run,name=%s,success=%s,extractor=%s",
			statsdPrefix,
			recipe.Name,
			"true",
			recipe.Source.Name,
		)
		recordIncrementMetric := fmt.Sprintf(
			"%s.runRecordCount,name=%s,success=%s,extractor=%s",
			statsdPrefix,
			recipe.Name,
			"true",
			recipe.Source.Name,
		)

		client := new(mockStatsdClient)
		client.On("Timing", timingMetric, int64(duration))
		client.On("Increment", incrementMetric)
		client.On("IncrementByValue", recordIncrementMetric, recordCount)
		defer client.AssertExpectations(t)

		monitor := metrics.NewStatsdMonitor(client, statsdPrefix)
		monitor.RecordRun(context.Background(), agent.Run{Recipe: recipe, DurationInMs: duration, RecordCount: recordCount, Success: true})
	})
}

func TestStatsdMonitorRecordPlugin(t *testing.T) {
	statsdPrefix := "testprefix"

	t.Run("should create metrics with the correct name and value", func(t *testing.T) {
		recipe := recipe.Recipe{
			Name: "test-recipe",
			Source: recipe.PluginRecipe{
				Name: "mysql",
			},
			Sinks: []recipe.PluginRecipe{
				{Name: "test-sink"},
			},
		}
		incrementMetric := fmt.Sprintf(
			"%s.%s,recipe_name=%s,name=%s,type=%s,success=%t",
			statsdPrefix,
			"runPlugin",
			recipe.Name,
			recipe.Sinks[0].Name,
			"sink",
			false,
		)

		client := new(mockStatsdClient)
		client.On("Increment", incrementMetric)
		defer client.AssertExpectations(t)

		monitor := metrics.NewStatsdMonitor(client, statsdPrefix)
		monitor.RecordPlugin(context.Background(), agent.PluginInfo{
			RecipeName: recipe.Name,
			PluginName: recipe.Sinks[0].Name,
			PluginType: "sink",
			Success:    false,
		})
	})

	t.Run("should set success field to true on success", func(t *testing.T) {
		recipe := recipe.Recipe{
			Name: "test-recipe",
			Source: recipe.PluginRecipe{
				Name: "bigquery",
			},
			Sinks: []recipe.PluginRecipe{
				{Name: "test-sink"},
			},
		}
		incrementMetric := fmt.Sprintf(
			"%s.%s,recipe_name=%s,name=%s,type=%s,success=%t",
			statsdPrefix,
			"runPlugin",
			recipe.Name,
			recipe.Sinks[0].Name,
			"sink",
			true,
		)

		client := new(mockStatsdClient)
		client.On("Increment", incrementMetric)
		defer client.AssertExpectations(t)

		monitor := metrics.NewStatsdMonitor(client, statsdPrefix)
		monitor.RecordPlugin(context.Background(),
			agent.PluginInfo{
				RecipeName: recipe.Name,
				PluginName: recipe.Sinks[0].Name,
				PluginType: "sink",
				Success:    true,
			})
	})
}

func TestNewStatsClient(t *testing.T) {
	t.Run("should return error for invalid address", func(t *testing.T) {
		_, err := metrics.NewStatsdClient("127.0.0.1")
		assert.Error(t, err)
	})
	t.Run("should return error for invalid port", func(t *testing.T) {
		_, err := metrics.NewStatsdClient("127.0.0.1:81A5")
		assert.Error(t, err)
	})
}
