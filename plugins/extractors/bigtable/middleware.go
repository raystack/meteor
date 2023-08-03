package bigtable

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/googleapis/gax-go/v2/apierror"
	"github.com/goto/meteor/utils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type AdminClientMW struct {
	tableDuration  metric.Int64Histogram
	tablesDuration metric.Int64Histogram
	next           AdminClient
	attributes     []attribute.KeyValue
}

type InstanceAdminClientMW struct {
	instancesDuration metric.Int64Histogram
	next              InstanceAdminClient
	attributes        []attribute.KeyValue
}

func WithAdminClientMW(projectID, instanceName string) func(AdminClient) AdminClient {
	meter := otel.Meter("github.com/goto/meteor/plugins/extractors/bigtable")

	tablesDuration, err := meter.Int64Histogram("meteor.bigtable.client.tables.duration", metric.WithUnit("ms"))
	handleOtelErr(err)

	tableDuration, err := meter.Int64Histogram("meteor.bigtable.client.table.duration", metric.WithUnit("ms"))
	handleOtelErr(err)

	return func(next AdminClient) AdminClient {
		return &AdminClientMW{
			tableDuration:  tableDuration,
			tablesDuration: tablesDuration,
			next:           next,
			attributes: []attribute.KeyValue{
				attribute.String("bt.project_id", projectID),
				attribute.String("bt.instance_name", instanceName),
			},
		}
	}
}

func WithInstanceAdminClientMW(projectID string) func(InstanceAdminClient) InstanceAdminClient {
	instancesDuration, err := otel.Meter("github.com/goto/meteor/plugins/extractors/bigtable").
		Int64Histogram("meteor.bigtable.client.instances.duration", metric.WithUnit("ms"))
	handleOtelErr(err)

	return func(next InstanceAdminClient) InstanceAdminClient {
		return &InstanceAdminClientMW{
			instancesDuration: instancesDuration,
			next:              next,
			attributes: []attribute.KeyValue{
				attribute.String("bt.project_id", projectID),
			},
		}
	}
}

func (o *AdminClientMW) Tables(ctx context.Context) (res []string, err error) {
	defer func(start time.Time) {
		attrs := o.attributes
		if err != nil {
			attrs = append(attrs, attribute.String("bt.error_code", apiErrReason(err)))
		}
		o.tablesDuration.Record(ctx,
			time.Since(start).Milliseconds(),
			metric.WithAttributes(attrs...))
	}(time.Now())

	return o.next.Tables(ctx)
}

func (o *AdminClientMW) TableInfo(ctx context.Context, table string) (res *bigtable.TableInfo, err error) {
	defer func(start time.Time) {
		attrs := append(o.attributes, attribute.String("bt.table_name", table))
		if err != nil {
			attrs = append(attrs, attribute.String("bt.error_code", apiErrReason(err)))
		}
		o.tableDuration.Record(ctx,
			time.Since(start).Milliseconds(),
			metric.WithAttributes(attrs...))
	}(time.Now())
	return o.next.TableInfo(ctx, table)
}

func (o *InstanceAdminClientMW) Instances(ctx context.Context) (res []*bigtable.InstanceInfo, err error) {
	defer func(start time.Time) {
		attrs := o.attributes
		if err != nil {
			attrs = append(o.attributes, attribute.String("bt.error_code", apiErrReason(err)))
		}

		o.instancesDuration.Record(ctx,
			time.Since(start).Milliseconds(),
			metric.WithAttributes(attrs...))
	}(time.Now())

	return o.next.Instances(ctx)
}

func apiErrReason(err error) string {
	reason := utils.StatusCode(err).String()
	var apiErr *apierror.APIError
	if errors.As(err, &apiErr) {
		reason = apiErr.Reason()
	}

	return reason
}

func handleOtelErr(err error) {
	if err != nil {
		otel.Handle(err)
	}
}
