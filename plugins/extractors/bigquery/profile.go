package bigquery

import "github.com/odpf/meteor/models/odpf/assets"

func (e *Extractor) buildTableProfile(tableURN string) (tp *assets.TableProfile) {
	var tableUsage int64
	var commonJoins []*assets.Join
	var filterConditions []string

	if e.config.IsCollectTableUsage && e.tableStats != nil {
		// table usage
		tableUsage = e.tableStats.TableUsage[tableURN]

		// common join
		if jdMapping, exist := e.tableStats.JoinDetail[tableURN]; exist {
			for joinedTableURN, jd := range jdMapping {
				var joinConditions []string
				for jc := range jd.Conditions {
					joinConditions = append(joinConditions, jc)
				}
				commonJoins = append(commonJoins, &assets.Join{
					Urn:        joinedTableURN,
					Count:      jd.Usage,
					Conditions: joinConditions,
				})
			}
		}

		// filter conditions
		if filterMapping, exist := e.tableStats.FilterConditions[tableURN]; exist {
			for filterExpression := range filterMapping {
				filterConditions = append(filterConditions, filterExpression)
			}
		}
	}

	tp = &assets.TableProfile{
		UsageCount: tableUsage,
		Joins:      commonJoins,
		Filters:    filterConditions,
	}

	return
}
