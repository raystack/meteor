package bigquery

import "github.com/odpf/meteor/models/odpf/assets"

func (e *Extractor) buildTableProfile(tableURN string) (tp *assets.TableProfile) {
	var tableUsage int64
	var commonJoins []*assets.TableCommonJoin

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
				commonJoins = append(commonJoins, &assets.TableCommonJoin{
					Urn:        joinedTableURN,
					Count:      jd.Usage,
					Conditions: joinConditions,
				})
			}
		}

	}

	tp = &assets.TableProfile{
		UsageCount: tableUsage,
		CommonJoin: commonJoins,
	}

	return
}
