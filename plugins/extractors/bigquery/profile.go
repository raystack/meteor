package bigquery

import (
	"cloud.google.com/go/bigquery"
	"github.com/raystack/meteor/plugins/extractors/bigquery/auditlog"
)

type tableProfile struct {
	UsageCount  int64                    `json:"usage_count,omitempty"`
	CommonJoins []map[string]interface{} `json:"common_joins,omitempty"`
	Filters     []string                 `json:"filters,omitempty"`
	TotalRows   int64                    `json:"total_rows,omitempty"`
}

func (e *Extractor) buildTableProfile(tableURN string, tableStats *auditlog.TableStats, md *bigquery.TableMetadata) tableProfile {
	var tableUsage int64
	var commonJoins []map[string]interface{}
	var filterConditions []string

	if e.config.IsCollectTableUsage && tableStats != nil {
		// table usage
		tableUsage = tableStats.TableUsage[tableURN]

		// common join
		if jdMapping, exist := tableStats.JoinDetail[tableURN]; exist {
			for joinedTableURN, jd := range jdMapping {
				var joinConditions []string
				for jc := range jd.Conditions {
					joinConditions = append(joinConditions, jc)
				}
				commonJoins = append(commonJoins, map[string]interface{}{
					"urn":        joinedTableURN,
					"count":      jd.Usage,
					"conditions": joinConditions,
				})
			}
		}

		// filter conditions
		if filterMapping, exist := tableStats.FilterConditions[tableURN]; exist {
			for filterExpression := range filterMapping {
				filterConditions = append(filterConditions, filterExpression)
			}
		}
	}

	return tableProfile{
		UsageCount:  tableUsage,
		CommonJoins: commonJoins,
		Filters:     filterConditions,
		TotalRows:   int64(md.NumRows),
	}
}
