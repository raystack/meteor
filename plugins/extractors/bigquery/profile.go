package bigquery

import "github.com/odpf/meteor/models/odpf/assets"

func (e *Extractor) buildTableProfile(tableURN string) (tp *assets.TableProfile) {
	var tableUsage int64
	var commonJoins []*assets.TableCommonJoin

	if e.config.IsCollectTableUsage && e.tableStats != nil {
		// table usage
		tableUsage = e.tableStats.TableUsage[tableURN]

		// common join
		if cjList, exist := e.tableStats.JoinUsage[tableURN]; exist {
			for cjURN, cjCount := range cjList {
				commonJoins = append(commonJoins, &assets.TableCommonJoin{
					Urn:   cjURN,
					Count: cjCount,
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
