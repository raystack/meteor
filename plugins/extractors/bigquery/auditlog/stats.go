package auditlog

import (
	"github.com/goto/meteor/plugins/extractors/bigquery/sqlparser"
	"github.com/pkg/errors"
)

type TableStats struct {
	TableUsage       map[string]int64
	JoinDetail       map[string]map[string]JoinDetail
	FilterConditions map[string]map[string]bool
	processedLog     *LogData
}

type JoinDetail struct {
	Usage      int64
	Conditions map[string]bool
}

func NewTableStats() *TableStats {
	ts := &TableStats{}
	ts.initPopulate()
	return ts
}

func (b *TableStats) initPopulate() {
	b.TableUsage = map[string]int64{}
	b.JoinDetail = map[string]map[string]JoinDetail{}
	b.FilterConditions = map[string]map[string]bool{}
}

func (b *TableStats) Populate(ld *LogData) (err error) {
	b.processedLog = ld

	// if 0, query is not involving table
	refTablesURN := b.processedLog.GetReferencedTablesURN()
	if len(refTablesURN) == 0 {
		err = errors.New("got empty referenced tables")
		return
	}
	// query must be there, otherwise it is not valid
	sqlQuery, err := b.processedLog.GetQuery()
	if err != nil {
		// log query not exist here
		err = errors.Wrap(err, "can't get query")
		return
	}

	jcs := sqlparser.ParseJoinConditions(sqlQuery)
	fcs := sqlparser.ParseFilterConditions(sqlQuery)

	// populate all data
	for _, rt := range refTablesURN {

		// single table usage
		b.populateTableUsage(rt)

		// no common join if only 1 referenced tables
		if len(refTablesURN) > 1 {
			b.populateJoinDetail(rt, refTablesURN, jcs)
		}

		b.populateFilterConditions(rt, fcs)
	}

	return
}

func (b *TableStats) populateTableUsage(tableURN string) {
	// single table usage
	if _, exist := b.TableUsage[tableURN]; !exist {
		b.TableUsage[tableURN] = 0
	}
	b.TableUsage[tableURN]++
}

func (b *TableStats) populateJoinDetail(tableURN string, refTablesURN []string, jcs []string) {
	if _, exist := b.JoinDetail[tableURN]; !exist {
		b.JoinDetail[tableURN] = map[string]JoinDetail{}
	}

	// join detail
	for _, selectedTableURN := range refTablesURN {
		if selectedTableURN == tableURN {
			continue
		}

		// init usage and conditions
		jd, exist := b.JoinDetail[tableURN][selectedTableURN]
		if !exist {
			jd.Usage = 1
		} else {
			// update usage
			jd.Usage++
		}
		b.JoinDetail[tableURN][selectedTableURN] = jd

		// ignore join conditions
		if len(jcs) == 0 {
			continue
		}

		// init conditions
		if jd.Conditions == nil {
			jd.Conditions = map[string]bool{}
		}

		for _, jc := range jcs {
			jd.Conditions[jc] = true
			b.JoinDetail[tableURN][selectedTableURN] = jd
		}

	}

}

func (b *TableStats) populateFilterConditions(tableURN string, fcs []string) {
	if len(fcs) == 0 {
		return
	}

	if _, exist := b.FilterConditions[tableURN]; !exist {
		b.FilterConditions[tableURN] = map[string]bool{}
	}

	for _, fc := range fcs {
		b.FilterConditions[tableURN][fc] = true
	}
}
