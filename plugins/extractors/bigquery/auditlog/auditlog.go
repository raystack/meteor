package auditlog

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/logging/logadmin"
	"github.com/goto/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	auditpb "google.golang.org/genproto/googleapis/cloud/audit"
	loggingpb "google.golang.org/genproto/googleapis/cloud/bigquery/logging/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

type Config struct {
	ProjectID           string
	ServiceAccountJSON  string
	IsCollectTableUsage bool
	UsagePeriodInDay    int64
	UsageProjectIDs     []string
}

const advancedFilterTemplate = `protoPayload.methodName="jobservice.jobcompleted" AND ` +
	`resource.type="bigquery_resource" AND NOT ` +
	`protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.query.query:(INFORMATION_SCHEMA OR __TABLES__) AND ` +
	`timestamp >= "%s" AND timestamp < "%s" AND %s`

type AuditLog struct {
	logger log.Logger
	client *logadmin.Client
	config Config
}

func New(logger log.Logger) *AuditLog {
	return &AuditLog{
		logger: logger,
	}
}

func (l *AuditLog) Init(ctx context.Context, opts ...InitOption) error {
	for _, opt := range opts {
		opt(l)
	}

	if len(l.config.UsageProjectIDs) == 0 {
		l.config.UsageProjectIDs = []string{l.config.ProjectID}
	}

	if l.client == nil {
		var err error
		l.client, err = l.createClient(ctx)
		if err != nil {
			return fmt.Errorf("create logadmin client: %w", err)
		}
	}

	return nil
}

func (l *AuditLog) createClient(ctx context.Context) (*logadmin.Client, error) {
	if l.config.ServiceAccountJSON == "" {
		l.logger.Info("credentials are not specified, creating logadmin using default credentials...")
		return logadmin.NewClient(ctx, l.config.ProjectID)
	}

	return logadmin.NewClient(ctx, l.config.ProjectID, option.WithCredentialsJSON([]byte(l.config.ServiceAccountJSON)))
}

func (l *AuditLog) Collect(ctx context.Context, tableID string) (*TableStats, error) {
	if l.client == nil {
		return nil, errors.New("auditlog client is nil")
	}

	filter := l.buildFilter(tableID)
	it := l.client.Entries(ctx,
		logadmin.ProjectIDs(l.config.UsageProjectIDs),
		logadmin.Filter(filter))

	l.logger.Info("getting logs in these projects", "projects", l.config.UsageProjectIDs)
	l.logger.Info("getting logs with the filter", "filter", filter)

	tableStats := NewTableStats()
	for {
		entry, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error iterating logEntries: %w", err)
		}

		logData, err := parsePayload(entry.Payload)
		if err != nil {
			l.logger.Warn("error parsing LogEntry payload", "err", err)
			continue
		}

		if errF := tableStats.Populate(logData); errF != nil {
			l.logger.Warn("error populating logdata", "err", errF)
			continue
		}
	}
	return tableStats, nil
}

func (l *AuditLog) buildFilter(tableID string) string {
	timeNow := time.Now().UTC()
	dayDuration := time.Duration(24*l.config.UsagePeriodInDay) * time.Hour
	timeFrom := timeNow.Add(-1 * dayDuration)

	timeNowFormatted := timeNow.Format(time.RFC3339)
	timeFromFormatted := timeFrom.Format(time.RFC3339)

	return fmt.Sprintf(advancedFilterTemplate, timeFromFormatted, timeNowFormatted, tableID)
}

func parsePayload(payload interface{}) (*LogData, error) {
	pl, ok := payload.(*auditpb.AuditLog)
	if !ok {
		return nil, errors.New("parse payload to AuditLog")
	}

	var ad loggingpb.AuditData
	if err := getAuditData(pl, &ad); err != nil {
		return nil, fmt.Errorf("get audit data from metadata: %w", err)
	}

	ld := &LogData{&ad}
	if err := ld.validateAuditData(); err != nil {
		return nil, err
	}

	return ld, nil
}

func getAuditData(pl *auditpb.AuditLog, ad *loggingpb.AuditData) error {
	// ServiceData is deprecated and suggested to be replaced with Metadata
	// But in some logs, ServiceData is still being used
	//nolint:staticcheck
	if pl.GetServiceData() != nil {
		// if ServiceData is not nil, the log is still using the old one
		return getAuditDataFromServiceData(pl, ad)
	}

	// perhaps with metadata
	return getAuditDataFromMetadata(pl, ad)
}

func getAuditDataFromServiceData(pl *auditpb.AuditLog, ad *loggingpb.AuditData) error {
	//nolint:staticcheck
	if err := pl.GetServiceData().UnmarshalTo(ad); err != nil {
		return fmt.Errorf("marshal service data to audit data: %w", err)
	}
	return nil
}

func getAuditDataFromMetadata(pl *auditpb.AuditLog, ad *loggingpb.AuditData) error {
	if pl.GetMetadata() == nil {
		return errors.New("metadata field is nil")
	}

	mdJSON, err := pl.GetMetadata().MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshal payload metadata: %w", err)
	}

	if err := protojson.Unmarshal(mdJSON, ad); err != nil {
		return fmt.Errorf("parse service data to Audit: %w", err)
	}

	return nil
}
