package auditlog

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/logging/logadmin"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	auditpb "google.golang.org/genproto/googleapis/cloud/audit"
	loggingpb "google.golang.org/genproto/googleapis/cloud/bigquery/logging/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

type Config struct {
	ProjectID           string `mapstructure:"project_id" validate:"required"`
	ServiceAccountJSON  string `mapstructure:"service_account_json"`
	IsCollectTableUsage bool   `mapstructure:"collect_table_usage" default:"false"`
	UsagePeriodInDay    int64  `mapstructure:"usage_period_in_day" default:"7"`
}

const advancedFilterTemplate = `protoPayload.methodName="jobservice.jobcompleted" AND ` +
	`resource.type="bigquery_resource" AND NOT ` +
	`protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.query.query:(INFORMATION_SCHEMA OR __TABLES__) AND ` +
	`timestamp >= "%s" AND timestamp < "%s"`

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

func (l *AuditLog) Init(ctx context.Context, cfg Config) (err error) {
	l.config = cfg
	l.client, err = l.createClient(ctx)
	if err != nil {
		err = errors.Wrap(err, "failed to create logadmin client")
		return
	}

	return
}

func (l *AuditLog) createClient(ctx context.Context) (client *logadmin.Client, err error) {
	if l.config.ServiceAccountJSON == "" {
		l.logger.Info("credentials are not specified, creating logadmin using default credentials...")
		client, err = logadmin.NewClient(ctx, l.config.ProjectID)
		return
	}

	client, err = logadmin.NewClient(ctx, l.config.ProjectID, option.WithCredentialsJSON([]byte(l.config.ServiceAccountJSON)))
	if err != nil {
		err = errors.New("client is nil, failed initiating client")
	}
	return
}

func (l *AuditLog) Collect(ctx context.Context) (tableStats *TableStats, err error) {
	tableStats = NewTableStats()

	filter := l.buildFilter()
	it := l.client.Entries(ctx,
		logadmin.ProjectIDs([]string{l.config.ProjectID}),
		logadmin.Filter(filter))

	l.logger.Info("getting logs with the filter", "filter", filter)

	for {
		entry, errF := it.Next()
		if errF == iterator.Done {
			break
		}
		if errF != nil {
			err = errors.Wrap(errF, "error iterating logEntries")
			break
		}
		logData, errF := parsePayload(entry.Payload)
		if errF != nil {
			l.logger.Warn("error parsing LogEntry payload", "err", errF, "payload", entry.Payload)
			continue
		}

		if errF := tableStats.Populate(logData); errF != nil {
			l.logger.Warn("error populating logdata", "err", errF)
			continue
		}
	}
	return
}

func (l *AuditLog) buildFilter() string {

	timeNow := time.Now().UTC()
	dayDuration := time.Duration(24*l.config.UsagePeriodInDay) * time.Hour
	timeFrom := timeNow.Add(-1 * dayDuration)

	timeNowFormatted := timeNow.Format(time.RFC3339)
	timeFromFormatted := timeFrom.Format(time.RFC3339)

	return fmt.Sprintf(advancedFilterTemplate, timeFromFormatted, timeNowFormatted)
}

func parsePayload(payload interface{}) (ld *LogData, err error) {

	ad := &loggingpb.AuditData{}
	pl, ok := payload.(*auditpb.AuditLog)
	if !ok {
		err = errors.New("cannot parse payload to AuditLog")
		return
	}

	if errPB := getAuditData(pl, ad); errPB != nil {
		err = errors.Wrap(err, "failed to get audit data from metadata")
		return
	}

	ld = &LogData{ad}
	err = ld.validateAuditData()
	return
}

func getAuditData(pl *auditpb.AuditLog, ad *loggingpb.AuditData) (err error) {
	// ServiceData is deprecated and suggested to be replaced with Metadata
	// But in some logs, ServiceData is still being used
	//nolint:staticcheck
	if pl.GetServiceData() != nil {
		// if ServiceData is not nil, the log is still using the old one
		if errPB := getAuditDataFromServiceData(pl, ad); errPB != nil {
			err = errors.Wrap(err, "failed to get audit data from service data")
			return
		}
	}

	// perhaps with metadata
	if errPB := getAuditDataFromMetadata(pl, ad); errPB != nil {
		err = errors.Wrap(err, "failed to get audit data from metadata")
		return
	}
	err = errors.New("AuditData not found")
	return
}

func getAuditDataFromServiceData(pl *auditpb.AuditLog, ad *loggingpb.AuditData) (err error) {
	//nolint:staticcheck
	if errPB := pl.GetServiceData().UnmarshalTo(ad); errPB != nil {
		err = errors.New("failed to marshal service data to audit data")
		return
	}
	return
}

func getAuditDataFromMetadata(pl *auditpb.AuditLog, ad *loggingpb.AuditData) (err error) {
	mdJSON, err := pl.GetMetadata().MarshalJSON()
	if err != nil {
		err = errors.New("cannot marshal payload metadata")
		return
	}

	if errPB := protojson.Unmarshal(mdJSON, ad); errPB != nil {
		err = errors.New("cannot parse service data to Audit")
		return
	}
	return
}
