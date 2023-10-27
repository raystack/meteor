//go:build plugins
// +build plugins

package bigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/bigquery/auditlog"
	"github.com/stretchr/testify/assert"
)

func TestBuildTableProfile(t *testing.T) {
	tableURN := plugins.BigQueryURN("project1", "dataset1", "table1")
	t.Run("table profile usage related fields are empty if usage collection is disabled", func(t *testing.T) {
		var tableStats *auditlog.TableStats
		extr := &Extractor{
			config: Config{
				IsCollectTableUsage: false,
			},
		}

		tp := extr.buildTableProfile(tableURN, tableStats, &bigquery.TableMetadata{})

		assert.Empty(t, tp.UsageCount)
		assert.Empty(t, tp.CommonJoins)
	})

	t.Run("table profile usage related fields are empty if table stats is nil", func(t *testing.T) {
		extr := &Extractor{
			config: Config{
				IsCollectTableUsage: true,
			},
		}

		tp := extr.buildTableProfile(tableURN, nil, &bigquery.TableMetadata{})

		assert.Empty(t, tp.UsageCount)
		assert.Empty(t, tp.CommonJoins)
	})

	t.Run("table profile usage related fields are populated if table stats is not nil and usage collection is enabled", func(t *testing.T) {
		tableStats := &auditlog.TableStats{
			TableUsage: map[string]int64{
				plugins.BigQueryURN("project1", "dataset1", "table1"): 5,
				plugins.BigQueryURN("project2", "dataset1", "table1"): 3,
				plugins.BigQueryURN("project3", "dataset1", "table1"): 1,
			},
			JoinDetail: map[string]map[string]auditlog.JoinDetail{
				plugins.BigQueryURN("project1", "dataset1", "table1"): {
					plugins.BigQueryURN("project2", "dataset1", "table1"): auditlog.JoinDetail{
						Usage: 1,
					},
					plugins.BigQueryURN("project3", "dataset1", "table1"): auditlog.JoinDetail{
						Usage: 3,
					},
					plugins.BigQueryURN("project4", "dataset1", "table1"): auditlog.JoinDetail{
						Usage: 1,
						Conditions: map[string]bool{
							"ON t1.somefield = t2.anotherfield": true,
						},
					},
				},
			},
			FilterConditions: map[string]map[string]bool{
				plugins.BigQueryURN("project1", "dataset1", "table1"): {
					"WHERE t1.somefield = 'somevalue'": true,
				},
			},
		}

		extr := &Extractor{
			config: Config{
				IsCollectTableUsage: true,
			},
		}

		tp := extr.buildTableProfile(tableURN, tableStats, &bigquery.TableMetadata{
			NumRows: 42,
		})

		assert.EqualValues(t, 5, tp.UsageCount)
		assert.Contains(t, tp.CommonJoins, &v1beta2.TableCommonJoin{
			Urn:   plugins.BigQueryURN("project2", "dataset1", "table1"),
			Count: 1,
		})
		assert.Contains(t, tp.CommonJoins, &v1beta2.TableCommonJoin{
			Urn:   plugins.BigQueryURN("project3", "dataset1", "table1"),
			Count: 3,
		})
		assert.Contains(t, tp.CommonJoins, &v1beta2.TableCommonJoin{
			Urn:        plugins.BigQueryURN("project4", "dataset1", "table1"),
			Count:      1,
			Conditions: []string{"ON t1.somefield = t2.anotherfield"},
		})
		assert.Contains(t, tp.Filters, "WHERE t1.somefield = 'somevalue'")
		assert.Equal(t, tp.TotalRows, int64(42))
	})
}
