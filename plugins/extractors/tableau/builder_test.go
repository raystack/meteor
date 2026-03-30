//go:build plugins
// +build plugins

package tableau

import (
	"testing"

	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

func TestBuildLineageResource(t *testing.T) {
	t.Run("building bigquery DatabaseServer resource from interface", func(t *testing.T) {
		table := &Table{
			ID:       "id_table_1",
			Name:     "table1",
			FullName: "[database_server.access_data].[table1]",
			Schema:   "access_data",
			Database: Database{
				"id":             "db_server",
				"name":           "database_server",
				"connectionType": "bigquery",
				"hostName":       "",
				"port":           -1,
				"service":        "",
			},
		}
		e := New(testutils.Logger)
		urn, err := e.buildLineageResourceURN(table)

		assert.Nil(t, err)
		assert.Equal(t, "urn:bigquery:database_server:table:database_server:access_data.table1", urn)
	})

	t.Run("building other DatabaseServer resource from interface", func(t *testing.T) {
		table := &Table{
			ID:     "id_table_1",
			Name:   "table1",
			Schema: "",
			Database: Database{
				"id":             "db_server",
				"name":           "database_server",
				"connectionType": "postgres",
				"hostName":       "localhost",
				"port":           5432,
				"service":        "service",
			},
		}
		e := New(testutils.Logger)
		urn, err := e.buildLineageResourceURN(table)

		assert.Nil(t, err)
		assert.Equal(t, "urn:postgres:localhost:5432:table:database_server.table1", urn)
	})

	t.Run("building CloudFile resource from interface", func(t *testing.T) {
		table := &Table{
			ID:     "id_table",
			Name:   "table_name",
			Schema: "",
			Database: Database{
				"id":             "db_cloud_file",
				"name":           "database_cloud_file",
				"connectionType": "gcs",
				"provider":       "gcs",
			},
		}

		e := New(testutils.Logger)
		urn, err := e.buildLineageResourceURN(table)

		assert.Nil(t, err)
		assert.Equal(t, "urn:gcs:gcs:bucket:database_cloud_file/table_name", urn)
	})

	t.Run("building File resource from interface", func(t *testing.T) {
		table := &Table{
			ID:     "table_id",
			Name:   "table_name",
			Schema: "schema",
			Database: Database{
				"id":             "db_file",
				"name":           "database_file",
				"connectionType": "file",
				"filePath":       "/this/is/file",
			},
		}

		e := New(testutils.Logger)
		urn, err := e.buildLineageResourceURN(table)

		assert.Nil(t, err)
		assert.Equal(t, "urn:file:/this/is/file:bucket:database_file.table_name", urn)
	})

	t.Run("building WebDataConnector resource from interface", func(t *testing.T) {
		table := &Table{
			ID:     "table_id",
			Name:   "table_name",
			Schema: "schema",
			Database: Database{
				"id":             "db_wdc",
				"name":           "database_wdc",
				"connectionType": "web_data_connector",
				"connectorUrl":   "http://link_to_connector",
			},
		}

		e := New(testutils.Logger)
		urn, err := e.buildLineageResourceURN(table)

		assert.Nil(t, err)
		assert.Equal(t, "urn:web_data_connector:http://link_to_connector:table:database_wdc.table_name", urn)
	})

	t.Run("building nil table should return error", func(t *testing.T) {
		e := New(testutils.Logger)
		urn, err := e.buildLineageResourceURN(nil)
		assert.EqualError(t, err, "no table found")
		assert.Equal(t, "", urn)
	})

	t.Run("building maxcompute DatabaseServer resource with schema", func(t *testing.T) {
		table := &Table{
			ID:       "id_table_1",
			Name:     "table1",
			FullName: "mc_project.my_schema.table1",
			Schema:   "my_schema",
			Database: Database{
				"id":             "db_server",
				"name":           "mc_project",
				"connectionType": "maxcompute_jdbc",
				"hostName":       "",
				"port":           -1,
				"service":        "",
			},
		}
		e := New(testutils.Logger)
		urn, err := e.buildLineageResourceURN(table)

		assert.Nil(t, err)
		assert.Equal(t, "urn:maxcompute:mc_project:table:mc_project.my_schema.table1", urn)
	})

	t.Run("building maxcompute DatabaseServer resource without schema", func(t *testing.T) {
		table := &Table{
			ID:       "id_table_1",
			Name:     "table1",
			FullName: "mc_project.mc_project.table1",
			Schema:   "mc_project",
			Database: Database{
				"id":             "db_server",
				"name":           "mc_project",
				"connectionType": "maxcompute_jdbc",
				"hostName":       "",
				"port":           -1,
				"service":        "",
			},
		}
		e := New(testutils.Logger)
		urn, err := e.buildLineageResourceURN(table)

		assert.Nil(t, err)
		assert.Equal(t, "urn:maxcompute:mc_project:table:mc_project.default.table1", urn)
	})

	t.Run("building Unknown resource from interface", func(t *testing.T) {
		table := &Table{
			Name: "table_name",
			Database: Database{
				"id":             "an_id",
				"name":           "a_name",
				"connectionType": "conn_type",
			},
		}

		e := New(testutils.Logger)
		urn, err := e.buildLineageResourceURN(table)
		assert.EqualError(t, err, "cannot build lineage resource, database structure unknown")
		assert.Equal(t, "", urn)
	})
}

func TestBuildLineage(t *testing.T) {
	upstreamTables := []*Table{
		{
			ID:   "table_id_1",
			Name: "table_name_1",
			Database: Database{
				"id":             "db_1",
				"name":           "database_1",
				"connectionType": "postgres",
				"hostName":       "localhost",
				"port":           5432,
			},
		},
		{
			ID:   "table_id_2",
			Name: "table_name_2",
			Database: Database{
				"id":   "db_2",
				"name": "database_2",

				"connectionType": "gcs",
				"provider":       "gcs",
			},
		},
	}

	testDataWorkbook := Workbook{
		UpstreamTables: upstreamTables,
	}

	expectedURNs := []string{
		"urn:postgres:localhost:5432:table:database_1.table_name_1",
		"urn:gcs:gcs:bucket:database_2/table_name_2",
	}

	e := New(testutils.Logger)
	assert.Equal(t, expectedURNs, e.buildLineageURNs(testDataWorkbook.UpstreamTables))
}
