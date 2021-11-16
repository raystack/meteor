package auditlog

type TableStats struct {
	TableUsage   map[string]int64            `json:"table_usage_count"`
	JoinUsage    map[string]map[string]int64 `json:"join"`
	processedLog *LogData                    `json:"-"`
}

func NewTableStats() *TableStats {
	ts := &TableStats{}
	ts.initPopulate()
	return ts
}

func (b *TableStats) initPopulate() {
	b.TableUsage = map[string]int64{}
	b.JoinUsage = map[string]map[string]int64{}
}

func (b *TableStats) Populate(ld *LogData) {
	b.processedLog = ld

	// if 0, query is not involving table
	refTablesURN := b.processedLog.GetReferencedTablesURN()
	if len(refTablesURN) == 0 {
		return
	}

	// populate table usage
	b.populateTableUsage(refTablesURN)
	// populate join usage
	b.populateJoinUsage(refTablesURN)
}

func (b *TableStats) populateTableUsage(refTablesURN []string) {
	for _, rt := range refTablesURN {

		// single table usage
		if _, exist := b.TableUsage[rt]; !exist {
			b.TableUsage[rt] = 0
		}
		b.TableUsage[rt]++
	}
}

func (b *TableStats) populateJoinUsage(refTablesURN []string) {
	// no common join if only 1 referenced tables
	if len(refTablesURN) < 2 {
		return
	}

	for _, rtPtr := range refTablesURN {
		if _, exist := b.JoinUsage[rtPtr]; !exist {
			b.JoinUsage[rtPtr] = map[string]int64{}
		}

		for _, selectedTableURN := range refTablesURN {
			if selectedTableURN == rtPtr {
				continue
			}

			juCnt, exist := b.JoinUsage[rtPtr][selectedTableURN]
			if !exist {
				b.JoinUsage[rtPtr][selectedTableURN] = 1
				continue
			}
			juCnt++
			b.JoinUsage[rtPtr][selectedTableURN] = juCnt
		}
	}
}
