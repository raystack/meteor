package bigquery

import (
	"cloud.google.com/go/bigquery"
	v1beta2 "github.com/raystack/meteor/models/raystack/assets/v1beta2"
	"github.com/raystack/meteor/plugins/extractors/bigquery/auditlog"
)

func (e *Extractor) buildTableProfile(tableURN string, tableStats *auditlog.TableStats, md *bigquery.TableMetadata) *v1beta2.TableProfile {
	var tableUsage int64
	var commonJoins []*v1beta2.TableCommonJoin
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
				commonJoins = append(commonJoins, &v1beta2.TableCommonJoin{
					Urn:        joinedTableURN,
					Count:      jd.Usage,
					Conditions: joinConditions,
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

	return &v1beta2.TableProfile{
		UsageCount:  tableUsage,
		CommonJoins: commonJoins,
		Filters:     filterConditions,
		TotalRows:   int64(md.NumRows),
	}
}
