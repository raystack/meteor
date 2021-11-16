package auditlog

import (
	"github.com/odpf/meteor/models"
	loggingpb "google.golang.org/genproto/googleapis/cloud/bigquery/logging/v1"
)

var testDataRefTables1 = []string{
	models.TableURN("bigquery", "project1", "dataset1", "table1"),
	models.TableURN("bigquery", "project2", "dataset1", "table1"),
	models.TableURN("bigquery", "project3", "dataset1", "table1"),
}

var testDataRefTables2 = []string{
	models.TableURN("bigquery", "project1", "dataset1", "table1"),
	models.TableURN("bigquery", "project3", "dataset1", "table1"),
	models.TableURN("bigquery", "project4", "dataset1", "table1"),
}

var testDataRefTables3 = []string{
	models.TableURN("bigquery", "project1", "dataset1", "table1"),
	models.TableURN("bigquery", "project3", "dataset1", "table1"),
}

var testDataRefTables4 = []string{
	models.TableURN("bigquery", "project1", "dataset1", "table1"),
}

var testDataLogData1 = &LogData{
	&loggingpb.AuditData{
		JobCompletedEvent: &loggingpb.JobCompletedEvent{
			EventName: "",
			Job: &loggingpb.Job{
				JobStatistics: &loggingpb.JobStatistics{
					ReferencedTables: []*loggingpb.TableName{
						{
							ProjectId: "project1",
							DatasetId: "dataset1",
							TableId:   "table1",
						}, {
							ProjectId: "project2",
							DatasetId: "dataset1",
							TableId:   "table1",
						}, {
							ProjectId: "project3",
							DatasetId: "dataset1",
							TableId:   "table1",
						},
					},
				},
			},
		},
	},
}

var testDataLogData2 = &LogData{
	&loggingpb.AuditData{
		JobCompletedEvent: &loggingpb.JobCompletedEvent{
			EventName: "",
			Job: &loggingpb.Job{
				JobStatistics: &loggingpb.JobStatistics{
					ReferencedTables: []*loggingpb.TableName{
						{
							ProjectId: "project1",
							DatasetId: "dataset1",
							TableId:   "table1",
						}, {
							ProjectId: "project3",
							DatasetId: "dataset1",
							TableId:   "table1",
						}, {
							ProjectId: "project4",
							DatasetId: "dataset1",
							TableId:   "table1",
						},
					},
				},
			},
		},
	},
}

var testDataLogData3 = &LogData{
	&loggingpb.AuditData{
		JobCompletedEvent: &loggingpb.JobCompletedEvent{
			EventName: "",
			Job: &loggingpb.Job{
				JobStatistics: &loggingpb.JobStatistics{
					ReferencedTables: []*loggingpb.TableName{
						{
							ProjectId: "project1",
							DatasetId: "dataset1",
							TableId:   "table1",
						}, {
							ProjectId: "project3",
							DatasetId: "dataset1",
							TableId:   "table1",
						},
					},
				},
			},
		},
	},
}

var testDataLogData4 = &LogData{
	&loggingpb.AuditData{
		JobCompletedEvent: &loggingpb.JobCompletedEvent{
			EventName: "",
			Job: &loggingpb.Job{
				JobStatistics: &loggingpb.JobStatistics{
					ReferencedTables: []*loggingpb.TableName{
						{
							ProjectId: "project1",
							DatasetId: "dataset1",
							TableId:   "table1",
						},
					},
				},
			},
		},
	},
}

var testDataJoinUsage1234 = map[string]map[string]int64{
	models.TableURN("bigquery", "project1", "dataset1", "table1"): {
		models.TableURN("bigquery", "project2", "dataset1", "table1"): 1,
		models.TableURN("bigquery", "project3", "dataset1", "table1"): 3,
		models.TableURN("bigquery", "project4", "dataset1", "table1"): 1,
	},
	models.TableURN("bigquery", "project2", "dataset1", "table1"): {
		models.TableURN("bigquery", "project1", "dataset1", "table1"): 1,
		models.TableURN("bigquery", "project3", "dataset1", "table1"): 1,
	},
	models.TableURN("bigquery", "project3", "dataset1", "table1"): {
		models.TableURN("bigquery", "project1", "dataset1", "table1"): 3,
		models.TableURN("bigquery", "project2", "dataset1", "table1"): 1,
		models.TableURN("bigquery", "project4", "dataset1", "table1"): 1,
	},
	models.TableURN("bigquery", "project4", "dataset1", "table1"): {
		models.TableURN("bigquery", "project1", "dataset1", "table1"): 1,
		models.TableURN("bigquery", "project3", "dataset1", "table1"): 1,
	},
}

var testDataTableUsage1234 = map[string]int64{
	models.TableURN("bigquery", "project1", "dataset1", "table1"): 4,
	models.TableURN("bigquery", "project2", "dataset1", "table1"): 1,
	models.TableURN("bigquery", "project3", "dataset1", "table1"): 3,
	models.TableURN("bigquery", "project4", "dataset1", "table1"): 1,
}
