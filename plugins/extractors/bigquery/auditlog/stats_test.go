package auditlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	loggingpb "google.golang.org/genproto/googleapis/cloud/bigquery/logging/v1"
)

func TestCreatingTableStats(t *testing.T) {
	ts := NewTableStats()

	assert.EqualValues(t, ts.TableUsage, map[string]int64{})
	assert.EqualValues(t, ts.JoinDetail, map[string]map[string]JoinDetail{})
}

func TestPopulateIndividually(t *testing.T) {
	t.Run("populate table usage by counting every table in referenced tables", func(t *testing.T) {
		ts := NewTableStats()

		for _, td := range testDataRefTables1 {
			ts.populateTableUsage(td)
		}
		for _, td := range testDataRefTables2 {
			ts.populateTableUsage(td)
		}
		for _, td := range testDataRefTables3 {
			ts.populateTableUsage(td)
		}
		for _, td := range testDataRefTables4 {
			ts.populateTableUsage(td)
		}

		assert.EqualValues(t, testDataTableUsage1234, ts.TableUsage)
	})

	t.Run("populate join usage by counting every joined table in referenced tables", func(t *testing.T) {
		ts := NewTableStats()

		for _, td := range testDataRefTables1 {
			ts.populateJoinDetail(td, testDataRefTables1, nil)
		}
		for _, td := range testDataRefTables2 {
			ts.populateJoinDetail(td, testDataRefTables2, nil)
		}
		for _, td := range testDataRefTables3 {
			ts.populateJoinDetail(td, testDataRefTables3, nil)
		}
		for _, td := range testDataRefTables4 {
			ts.populateJoinDetail(td, testDataRefTables4, nil)
		}

		assert.EqualValues(t, testDataJoinUsage1234, ts.JoinDetail)
	})
}

func TestPopulateAll(t *testing.T) {
	t.Run("populate all usage data from log data", func(t *testing.T) {
		ts := NewTableStats()

		ts.Populate(testDataLogData1)
		ts.Populate(testDataLogData2)
		ts.Populate(testDataLogData3)
		ts.Populate(testDataLogData4)

		assert.EqualValues(t, testDataTableUsage1234, ts.TableUsage)
		assert.EqualValues(t, testDataJoinDetail1234, ts.JoinDetail)
		assert.EqualValues(t, testDataFilterCondition1234, ts.FilterConditions)
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
		assert.Empty(t, ts.JoinDetail)
		assert.Empty(t, ts.FilterConditions)
	})
}
