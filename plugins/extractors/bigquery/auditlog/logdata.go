package auditlog

import (
	"errors"
	"fmt"

	"github.com/goto/meteor/plugins"
	loggingpb "google.golang.org/genproto/googleapis/cloud/bigquery/logging/v1"
)

type LogData struct {
	*loggingpb.AuditData
}

func (ld *LogData) GetReferencedTablesURN() []string {
	var stats *loggingpb.JobStatistics
	if stats = ld.GetJobCompletedEvent().GetJob().GetJobStatistics(); stats == nil {
		return nil
	}

	var urns []string
	for _, rt := range stats.ReferencedTables {
		tableURN := plugins.BigQueryURN(rt.ProjectId, rt.DatasetId, rt.TableId)
		urns = append(urns, tableURN)
	}

	return urns
}

func (ld *LogData) GetQuery() (string, error) {
	if jobConfig := ld.GetJobCompletedEvent().GetJob().GetJobConfiguration(); jobConfig == nil {
		return "", errors.New("jobConfiguration is nil")
	}
	if jobConfigQuery := ld.GetJobCompletedEvent().GetJob().GetJobConfiguration().GetQuery(); jobConfigQuery == nil {
		return "", errors.New("jobConfigurationQuery is nil")
	}
	sqlQuery := ld.GetJobCompletedEvent().GetJob().GetJobConfiguration().GetQuery().GetQuery()
	if sqlQuery == "" {
		return "", errors.New("sql query is empty")
	}

	return sqlQuery, nil
}

func (ld *LogData) validateAuditData() error {
	if ld.GetJobCompletedEvent() == nil {
		return errors.New("jobCompletedEvent field not found")
	}

	job := ld.GetJobCompletedEvent().GetJob()
	if job == nil {
		return errors.New("jobCompletedEvent.job field not found")
	}

	// if referenced tables is empty, we don't count it
	stats := job.GetJobStatistics()
	if stats == nil {
		return errors.New("job statistics is nil")
	}

	if len(stats.ReferencedTables) == 0 {
		return errors.New("no referenced tables found")
	}

	jobStatus := job.GetJobStatus()
	if jobStatus == nil {
		return errors.New("jobCompletedEvent.job.jobStatus field not found")
	}

	jobState := jobStatus.GetState()
	if jobState == "" {
		return errors.New("jobCompletedEvent.job.jobStatus.state is empty")
	}

	// ignoring the job that has not finished
	if jobState != "DONE" {
		return errors.New("job status state is not DONE")
	}

	if jobStatus.GetError() != nil {
		if jobErrMsg := jobStatus.GetError().GetMessage(); jobErrMsg != "" {
			return fmt.Errorf("job status has error: %s", jobErrMsg)
		}
	}

	return nil
}
