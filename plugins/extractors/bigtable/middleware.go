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

type InstancesAdminClientMW struct {
	instancesDuration metric.Int64Histogram
	next              InstanceAdminClient
	attributes        []attribute.KeyValue
}

func WithAdminClientMW(next AdminClient, projectID, instanceName string) (AdminClient, error) {
	meter := otel.Meter("")

	tablesDuration, err := meter.Int64Histogram("meteor.bigtable.client.tables.duration", metric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}

	tableDuration, err := meter.Int64Histogram("meteor.bigtable.client.table.duration", metric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}

	return &AdminClientMW{
		tableDuration:  tableDuration,
		tablesDuration: tablesDuration,
		next:           next,
		attributes: []attribute.KeyValue{
			attribute.String("bt.project_id", projectID),
			attribute.String("bt.instance_name", instanceName),
		},
	}, nil
}

func WithInstancesAdminClientMW(next InstanceAdminClient, projectID string) (InstanceAdminClient, error) {
	meter := otel.Meter("")

	instancesDuration, err := meter.Int64Histogram("meteor.bigtable.client.instances.duration", metric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}

	return &InstancesAdminClientMW{
		instancesDuration: instancesDuration,
		next:              next,
		attributes: []attribute.KeyValue{
			attribute.String("bt.project_id", projectID),
		},
	}, nil
}

func (o *AdminClientMW) Tables(ctx context.Context) (res []string, err error) {
	defer func(start time.Time) {
		attrs := o.attributes
		if err != nil {
			attrs = append(attrs, attribute.String("bt.error_code", getAPIErrReason(err)))
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
			attrs = append(attrs, attribute.String("bt.error_code", getAPIErrReason(err)))
		}
		o.tableDuration.Record(ctx,
			time.Since(start).Milliseconds(),
			metric.WithAttributes(attrs...))
	}(time.Now())
	return o.next.TableInfo(ctx, table)
}

func (o *InstancesAdminClientMW) Instances(ctx context.Context) (res []*bigtable.InstanceInfo, err error) {
	defer func(start time.Time) {
		attrs := o.attributes
		if err != nil {
			attrs = append(o.attributes, attribute.String("bt.error_code", getAPIErrReason(err)))
		}

		o.instancesDuration.Record(ctx,
			time.Since(start).Milliseconds(),
			metric.WithAttributes(attrs...))
	}(time.Now())

	return o.next.Instances(ctx)
}

func getAPIErrReason(err error) string {
	reason := utils.StatusCode(err).String()
	var apiErr *apierror.APIError
	if errors.As(err, &apiErr) {
		reason = apiErr.Reason()
	}

	return reason
}
