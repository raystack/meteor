package metabase_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/odpf/meteor/models"
	testutils "github.com/odpf/meteor/test/utils"
	"github.com/pkg/errors"

	"github.com/nsf/jsondiff"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/metabase"
	"github.com/odpf/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	host = "https://my-metabase.com"
)

func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		client := new(mockClient)
		config := map[string]interface{}{
			"username": "user",
			"host":     "",
		}
		err := metabase.New(client, testutils.Logger).Init(context.TODO(), config)

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
	t.Run("should authenticate with client if config is valid", func(t *testing.T) {
		config := map[string]interface{}{
			"username":   "user",
			"host":       "sample-host",
			"password":   "sample-password",
			"session_id": "sample-session",
		}

		client := new(mockClient)
		client.On("Authenticate", "sample-host", "user", "sample-password", "sample-session").Return(nil)

		err := metabase.New(client, testutils.Logger).Init(context.TODO(), config)
		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should return dashboard model", func(t *testing.T) {
		dashboards := getDashboardList(t)
		dashboard_1 := getDashboard(t, 1)

		client := new(mockClient)
		client.On("Authenticate", host, "test-user", "test-pass", "").Return(nil)
		client.On("GetDashboards").Return(dashboards, nil)
		client.On("GetDashboard", 1).Return(dashboard_1, nil)
		defer client.AssertExpectations(t)

		emitter := mocks.NewEmitter()
		extr := metabase.New(client, testutils.Logger)
		err := extr.Init(context.TODO(), map[string]interface{}{
			"host":     host,
			"username": "test-user",
			"password": "test-pass",
		})
		if err != nil {
			t.Fatal(err)
		}

		err = extr.Extract(context.TODO(), emitter.Push)
		assert.NoError(t, err)

		records := emitter.Get()
		var actuals []models.Metadata
		for _, r := range records {
			actuals = append(actuals, r.Data())
		}

		assertWithFile(t, "./testdata/expected.json", actuals)
	})
}

func getDashboardList(t *testing.T) []metabase.Dashboard {
	var dashboards []metabase.Dashboard
	err := readFromFiles("./testdata/dashboards.json", &dashboards)
	if err != nil {
		t.Fatalf("error reading dashboards: %s", err.Error())
	}

	return dashboards
}

func getDashboard(t *testing.T, id int) metabase.Dashboard {
	var dashboard metabase.Dashboard
	filePath := fmt.Sprintf("./testdata/dashboard_%d.json", id)
	err := readFromFiles(filePath, &dashboard)
	if err != nil {
		t.Fatalf("error reading %s: %s", filePath, err.Error())
	}

	return dashboard
}

func readFromFiles(path string, data interface{}) error {
	file, err := os.Open(path)
	if err != nil {
		return errors.Wrapf(err, "error opening \"%s\"", path)
	}
	err = json.NewDecoder(file).Decode(&data)
	if err != nil {
		return errors.Wrapf(err, "error decoding \"%s\"", path)
	}

	return nil
}

func assertWithFile(t *testing.T, filePath string, actual interface{}) {
	var expected interface{}
	err := readFromFiles(filePath, &expected)
	if err != nil {
		t.Fatal(err)
	}

	actualBytes, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	expectedBytes, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}

	options := jsondiff.DefaultConsoleOptions()
	diff, report := jsondiff.Compare(expectedBytes, actualBytes, &options)
	if diff != jsondiff.FullMatch {
		t.Errorf("values do not match:\n %s", report)
	}
}

type mockClient struct {
	mock.Mock
}

func (m *mockClient) Authenticate(host, username, password, sessionID string) error {
	args := m.Called(host, username, password, sessionID)
	return args.Error(0)
}

func (m *mockClient) GetCards() ([]metabase.Card, error) {
	args := m.Called()
	return args.Get(0).([]metabase.Card), args.Error(1)
}

func (m *mockClient) GetDashboards() ([]metabase.Dashboard, error) {
	args := m.Called()
	return args.Get(0).([]metabase.Dashboard), args.Error(1)
}

func (m *mockClient) GetDashboard(id int) (metabase.Dashboard, error) {
	args := m.Called(id)
	return args.Get(0).(metabase.Dashboard), args.Error(1)
}
