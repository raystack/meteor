//go:build plugins
// +build plugins

package tableau

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/dnaeon/go-vcr/v2/recorder"
	"github.com/stretchr/testify/assert"
)

var testConfig = Config{
	Host:     "https://server.tableau.com",
	Version:  "3.12",
	Username: "meteor_user",
	Password: "xxxxxxxxxx",
	Sitename: "testdev550928",
}

func TestInit(t *testing.T) {
	t.Run("initializing client success", func(t *testing.T) {
		r, err := recorder.New("fixtures/init_client_success")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err = r.Stop()
		}()

		cl := NewClient(&http.Client{
			Transport: r,
		})

		err = cl.Init(context.TODO(), testConfig)
		assert.Nil(t, err)
	})

	t.Run("initializing client failed when credential is invalid", func(t *testing.T) {
		cl := NewClient(nil)

		err := cl.Init(context.TODO(), Config{
			Host:     "invalidhost",
			Version:  "3.12",
			Username: "invalid_user",
			Password: "xxxxxxxxxx",
			Sitename: "invalid_site",
		})
		assert.EqualError(t, err, "fetch auth token: generate response: Post \"invalidhost/api/3.12/auth/signin\": unsupported protocol scheme \"\"")
	})
}

func TestGetAllProjects(t *testing.T) {
	r, err := recorder.New("fixtures/get_projects_rest")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = r.Stop()
	}()

	cl := NewClient(&http.Client{
		Transport: r,
	})

	err = cl.Init(context.TODO(), testConfig)
	assert.Nil(t, err)

	expectedAPIResponse, err := testDataGetAllProjects(t)
	assert.Nil(t, err)

	projects, err := cl.GetAllProjects(context.TODO())
	assert.Nil(t, err)
	assert.Equal(t, expectedAPIResponse, projects)
}

func TestGetWorkbooksByProjectName(t *testing.T) {
	r, err := recorder.New("fixtures/get_workbooks_by_project_graphql_client")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = r.Stop()
	}()

	cl := NewClient(&http.Client{
		Transport: r,
	})

	err = cl.Init(context.TODO(), testConfig)
	assert.Nil(t, err)

	expectedAPIResponse, err := testDataGetWorkbooksByProjectName(t)
	assert.Nil(t, err)

	workbooks, err := cl.GetDetailedWorkbooksByProjectName(context.TODO(), "Samples")
	assert.Nil(t, err)
	assert.Equal(t, expectedAPIResponse, workbooks)
}

func testDataGetWorkbooksByProjectName(t *testing.T) (wbs []*Workbook, err error) {
	byteString, err := os.ReadFile("testdata/workbooks_by_project_response.json")
	if err != nil {
		t.Fatal(err)
	}

	var response responseGraphQL
	if err = json.Unmarshal([]byte(byteString), &response); err != nil {
		err = fmt.Errorf("parse: %s: %w", byteString, err)
	}

	wbs = response.Data.Workbooks
	return
}

func testDataGetAllProjects(t *testing.T) (ps []*Project, err error) {
	byteString, err := os.ReadFile("testdata/projects_response.json")
	if err != nil {
		t.Fatal(err)
	}

	var response responseProject
	if err = json.Unmarshal([]byte(byteString), &response); err != nil {
		err = fmt.Errorf("parse: %s: %w", byteString, err)
		return
	}

	ps = response.Projects.Projects
	return
}
