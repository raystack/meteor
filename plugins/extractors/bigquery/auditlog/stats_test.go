package auditlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	loggingpb "google.golang.org/genproto/googleapis/cloud/bigquery/logging/v1"
)

func TestCreatingTableStats(t *testing.T) {
	ts := NewTableStats()

	assert.EqualValues(t, ts.TableUsage, map[string]int64{})
	assert.EqualValues(t, ts.JoinUsage, map[string]map[string]int64{})
}

func TestPopulate(t *testing.T) {
	t.Run("populate table usage by counting every table in referenced tables", func(t *testing.T) {
		ts := NewTableStats()

		ts.populateTableUsage(testDataRefTables1)
		ts.populateTableUsage(testDataRefTables2)
		ts.populateTableUsage(testDataRefTables3)
		ts.populateTableUsage(testDataRefTables4)

		assert.EqualValues(t, testDataTableUsage1234, ts.TableUsage)
	})

	t.Run("populate join usage by counting every table occurences in referenced tables", func(t *testing.T) {
		ts := NewTableStats()

		ts.populateJoinUsage(testDataRefTables1)
		ts.populateJoinUsage(testDataRefTables2)
		ts.populateJoinUsage(testDataRefTables3)
		ts.populateJoinUsage(testDataRefTables4)

		assert.EqualValues(t, testDataJoinUsage1234, ts.JoinUsage)
	})

	t.Run("populate all usage data from log data", func(t *testing.T) {
		ts := NewTableStats()

		ts.Populate(testDataLogData1)
		ts.Populate(testDataLogData2)
		ts.Populate(testDataLogData3)
		ts.Populate(testDataLogData4)

		assert.EqualValues(t, testDataTableUsage1234, ts.TableUsage)
		assert.EqualValues(t, testDataJoinUsage1234, ts.JoinUsage)
	})

	t.Run("not populating table stats if no referenced tables found in log data", func(t *testing.T) {
		ld := &LogData{
			&loggingpb.AuditData{
				JobCompletedEvent: &loggingpb.JobCompletedEvent{
					EventName: "",
					Job: &loggingpb.Job{
						JobStatistics: &loggingpb.JobStatistics{
							ReferencedTables: []*loggingpb.TableName{},
						},
					},
				},
			},
		}

		ts := NewTableStats()

		ts.Populate(ld)

		assert.Empty(t, ts.TableUsage)
		assert.Empty(t, ts.JoinUsage)
	})
}
