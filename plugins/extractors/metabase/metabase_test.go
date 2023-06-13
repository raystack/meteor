//go:build plugins
// +build plugins

package metabase_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/metabase"
	m "github.com/goto/meteor/plugins/extractors/metabase/models"
	"github.com/goto/meteor/test/mocks"
	testutils "github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	host     = "https://my-metabase.com"
	urnScope = "test-metabase"
)

func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		client := new(mockClient)
		config := map[string]interface{}{
			"host":           "sample-host",
			"instance_label": "my-metabase",
		}
		err := metabase.New(client, testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope:  urnScope,
			RawConfig: config,
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
	t.Run("should authenticate with client if config is valid", func(t *testing.T) {
		config := map[string]interface{}{
			"host":           "sample-host",
			"instance_label": "my-metabase",
			"username":       "user",
			"password":       "sample-password",
		}

		client := new(mockClient)
		client.On("Authenticate", "sample-host", "user", "sample-password", "").Return(nil)

		err := metabase.New(client, testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope:  urnScope,
			RawConfig: config,
		})
		assert.NoError(t, err)
	})
	t.Run("should allow session_id to replace username and password", func(t *testing.T) {
		config := map[string]interface{}{
			"host":           "sample-host",
			"instance_label": "my-metabase",
			"session_id":     "sample-session",
		}

		client := new(mockClient)
		client.On("Authenticate", "sample-host", "", "", "sample-session").Return(nil)

		err := metabase.New(client, testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope:  urnScope,
			RawConfig: config,
		})
		assert.NoError(t, err)
	})
	t.Run("should return error on authentication failure", func(t *testing.T) {
		config := map[string]interface{}{
			"host":           "sample-host",
			"instance_label": "my-metabase",
			"session_id":     "sample-session",
		}

		client := new(mockClient)
		client.On("Authenticate", "sample-host", "", "", "sample-session").Return(errors.New("some error"))

		err := metabase.New(client, testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope:  urnScope,
			RawConfig: config,
		})
		assert.ErrorContains(t, err, "initiate client")
	})
}

func TestExtract(t *testing.T) {
	t.Run("should return dashboard model", func(t *testing.T) {
		dashboards := getDashboardList(t)
		dashboard1 := getDashboard(t, 1)

		client := new(mockClient)
		client.On("Authenticate", host, "test-user", "test-pass", "").Return(nil)
		client.On("GetDashboards").Return(dashboards, nil)
		client.On("GetDashboard", 1).Return(dashboard1, nil)
		client.On("GetTable", 2).Return(getTable(t, 2), nil).Once()
		client.On("GetDatabase", 2).Return(getDatabase(t, 2), nil).Twice()
		client.On("GetTable", 5).Return(getTable(t, 5), nil).Once()
		client.On("GetDatabase", 3).Return(getDatabase(t, 3), nil).Once()
		defer client.AssertExpectations(t)

		emitter := mocks.NewEmitter()
		extr := metabase.New(client, plugins.GetLog())
		err := extr.Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"host":           host,
				"username":       "test-user",
				"password":       "test-pass",
				"instance_label": "my-metabase",
			},
		})
		assert.NoError(t, err)

		err = extr.Extract(context.TODO(), emitter.Push)
		assert.NoError(t, err)

		actuals := emitter.GetAllData()
		testutils.AssertProtosWithJSONFile(t, "./testdata/expected.json", actuals)
	})

	t.Run("should return error when failed get dashboard list", func(t *testing.T) {
		expectedErr := errors.New("some error")
		client := new(mockClient)
		client.On("Authenticate", host, "test-user", "test-pass", "").Return(nil)
		client.On("GetDashboards").Return([]m.Dashboard{}, expectedErr)

		emitter := mocks.NewEmitter()
		extr := metabase.New(client, plugins.GetLog())
		err := extr.Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"host":           host,
				"username":       "test-user",
				"password":       "test-pass",
				"instance_label": "my-metabase",
			},
		})
		assert.NoError(t, err)

		err = extr.Extract(context.TODO(), emitter.Push)
		assert.ErrorContains(t, err, "fetch dashboard list")
	})

	t.Run("should return no error when failed get dashboard", func(t *testing.T) {
		expectedErr := errors.New("some error")
		client := new(mockClient)
		client.On("Authenticate", host, "test-user", "test-pass", "").Return(nil)
		client.On("GetDashboards").Return([]m.Dashboard{
			{ID: 1, Name: "Dashboard 1"},
		}, nil)
		client.On("GetDashboard", 1).Return(m.Dashboard{}, expectedErr)

		emitter := mocks.NewEmitter()
		extr := metabase.New(client, plugins.GetLog())
		err := extr.Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"host":           host,
				"username":       "test-user",
				"password":       "test-pass",
				"instance_label": "my-metabase",
			},
		})
		assert.NoError(t, err)

		err = extr.Extract(context.TODO(), emitter.Push)
		assert.NoError(t, err)
		actuals := emitter.GetAllData()
		testutils.AssertEqualProtos(t, []*v1beta2.Asset{}, actuals)
	})
}

func getDashboardList(t *testing.T) []m.Dashboard {
	var dashboards []m.Dashboard
	err := readFromFiles("./testdata/dashboards.json", &dashboards)
	if err != nil {
		t.Fatalf("error reading dashboards: %s", err.Error())
	}

	return dashboards
}

func getDashboard(t *testing.T, id int) m.Dashboard {
	var dashboard m.Dashboard
	filePath := fmt.Sprintf("./testdata/dashboard_%d.json", id)
	err := readFromFiles(filePath, &dashboard)
	if err != nil {
		t.Fatalf("error reading %s: %s", filePath, err.Error())
	}

	return dashboard
}

func getDatabase(t *testing.T, id int) m.Database {
	var database m.Database
	filePath := fmt.Sprintf("./testdata/database_%d.json", id)
	err := readFromFiles(filePath, &database)
	if err != nil {
		t.Fatalf("error reading %s: %s", filePath, err.Error())
	}

	return database
}

func getTable(t *testing.T, id int) m.Table {
	var table m.Table
	filePath := fmt.Sprintf("./testdata/table_%d.json", id)
	err := readFromFiles(filePath, &table)
	if err != nil {
		t.Fatalf("error reading %s: %s", filePath, err.Error())
	}

	return table
}

func readFromFiles(path string, data interface{}) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening %q: %w", path, err)
	}
	err = json.NewDecoder(file).Decode(&data)
	if err != nil {
		return fmt.Errorf("decode %q: %w", path, err)
	}

	return nil
}

type mockClient struct {
	mock.Mock
}

func (c *mockClient) Authenticate(ctx context.Context, host, username, password, sessionID string) error {
	args := c.Called(host, username, password, sessionID)
	return args.Error(0)
}

func (c *mockClient) GetDashboards(context.Context) ([]m.Dashboard, error) {
	args := c.Called()
	return args.Get(0).([]m.Dashboard), args.Error(1)
}

func (c *mockClient) GetDashboard(ctx context.Context, id int) (m.Dashboard, error) {
	args := c.Called(id)
	return args.Get(0).(m.Dashboard), args.Error(1)
}

func (c *mockClient) GetDatabase(ctx context.Context, id int) (m.Database, error) {
	args := c.Called(id)
	return args.Get(0).(m.Database), args.Error(1)
}

func (c *mockClient) GetTable(ctx context.Context, id int) (m.Table, error) {
	args := c.Called(id)
	return args.Get(0).(m.Table), args.Error(1)
}

// This function compares two slices without concerning about the order
func assertResults(t *testing.T, expected, result []models.Record) {
	assert.Len(t, result, len(expected))

	expectedMap := make(map[string]*v1beta2.Asset)
	for _, record := range expected {
		expectedAsset := record.Data()
		expectedMap[expectedAsset.Urn] = expectedAsset
	}

	for _, record := range result {
		actualAsset := record.Data()
		assert.Contains(t, expectedMap, actualAsset.Urn)
		assert.Equal(t, expectedMap[actualAsset.Urn], actualAsset)

		// delete entry to make sure there is no duplicate
		delete(expectedMap, actualAsset.Urn)
	}
}
