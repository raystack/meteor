package bigquery

import (
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins/extractors/bigquery/auditlog"
)

func (e *Extractor) buildTableProfile(tableURN string, tableStats *auditlog.TableStats) (tp *assetsv1beta1.TableProfile) {
	var tableUsage int64
	var commonJoins []*assetsv1beta1.Join
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
				commonJoins = append(commonJoins, &assetsv1beta1.Join{
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

	tp = &assetsv1beta1.TableProfile{
		UsageCount: tableUsage,
		Joins:      commonJoins,
		Filters:    filterConditions,
	}

	return
}
