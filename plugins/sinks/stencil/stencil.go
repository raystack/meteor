package stencil

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
)

//go:embed README.md
var summary string

// Config holds the set of configuration options for the sink
type Config struct {
	Host        string `mapstructure:"host" validate:"required"`
	NamespaceID string `mapstructure:"namespace_id" validate:"required"`
	Format      string `mapstructure:"format" validate:"oneof=json avro" default:"json"`
}

var info = plugins.Info{
	Description: "Send metadata to stencil http service",
	Summary:     summary,
	Tags:        []string{"http", "sink"},
	SampleConfig: heredoc.Doc(`
	# The hostname of the stencil service
	host: https://stencil.com
	# The namespace ID of the stencil service
	namespace_id: myNamespace
	# The schema format in which data will sink to stencil
	format: avro
	`),
}

// httpClient holds the set of methods require for creating request
type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

// Sink manages the sinking of data to Stencil
type Sink struct {
	plugins.BasePlugin
	client httpClient
	config Config
	logger log.Logger
}

// New returns a pointer to an initialized Sink Object
func New(c httpClient, logger log.Logger) plugins.Syncer {
	s := &Sink{
		logger: logger,
		client: c,
	}
	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)

	return s
}

// Init initializes the sink
func (s *Sink) Init(ctx context.Context, config plugins.Config) error {
	return s.BasePlugin.Init(ctx, config)
}

// Sink helps to sink record to stencil
func (s *Sink) Sink(ctx context.Context, batch []models.Record) error {
	for _, record := range batch {
		asset := record.Data()
		if asset.Data == nil {
			continue
		}

		var table v1beta2.Table
		if err := asset.Data.UnmarshalTo(&table); err != nil {
			continue
		}

		s.logger.Info("sinking record to stencil", "record", asset.GetUrn())

		var (
			payload interface{}
			err     error
		)
		switch s.config.Format {
		case "avro":
			payload, err = s.buildAvroStencilPayload(asset, &table)
		case "json":
			payload, err = s.buildJsonStencilPayload(asset, &table)
		}

		if err != nil {
			return fmt.Errorf("build stencil payload: %w", err)
		}
		if err := s.send(ctx, asset.GetUrn(), payload); err != nil {
			return fmt.Errorf("send stencil payload: %w", err)
		}

		s.logger.Info("successfully sunk record to stencil", "record", asset.GetUrn())
	}

	return nil
}

// Close will be called once after everything is done
func (*Sink) Close() error { return nil }

// buildJsonStencilPayload build json stencil payload
func (s *Sink) buildJsonStencilPayload(asset *v1beta2.Asset, table *v1beta2.Table) (JsonSchema, error) {
	jsonProperties := buildJsonProperties(asset, table)

	record := JsonSchema{
		Id:         asset.GetUrn() + ".json",
		Schema:     "https://json-schema.org/draft/2020-12/schema",
		Title:      asset.GetName(),
		Type:       JSONTypeObject,
		Properties: jsonProperties,
	}

	return record, nil
}

// buildAvroStencilPayload build Json stencil payload
func (s *Sink) buildAvroStencilPayload(asset *v1beta2.Asset, table *v1beta2.Table) (AvroSchema, error) {
	avroFields := buildAvroFields(asset, table)

	record := AvroSchema{
		Type:      "record",
		Namespace: s.config.NamespaceID,
		Name:      asset.GetName(),
		Fields:    avroFields,
	}

	return record, nil
}

// send helps to pass data to stencil
func (s *Sink) send(ctx context.Context, tableURN string, record interface{}) error {
	payloadBytes, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// send request
	schemaID := strings.ReplaceAll(tableURN, "/", ".")
	url := fmt.Sprintf("%s/v1beta1/namespaces/%s/schemas/%s", s.config.Host, s.config.NamespaceID, schemaID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}

	if s.config.Format == "json" {
		req.Header.Add("X-Compatibility", "COMPATIBILITY_UNSPECIFIED")
	}

	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer plugins.DrainBody(res)

	if res.StatusCode == http.StatusCreated {
		return nil
	}

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	err = fmt.Errorf("stencil returns %d: %v", res.StatusCode, string(bodyBytes))
	switch code := res.StatusCode; {
	case code >= 500:
		return plugins.NewRetryError(err)
	default:
		return err
	}
}

// buildJsonProperties builds the json schema properties
func buildJsonProperties(asset *v1beta2.Asset, table *v1beta2.Table) map[string]JsonProperty {
	columnRecord := make(map[string]JsonProperty)
	service := asset.GetService()
	columns := table.GetColumns()
	if len(columns) == 0 {
		return nil
	}

	for _, column := range columns {
		dataType := typeToJSONSchemaType(service, column.DataType)
		columnType := []JSONType{dataType}

		if column.IsNullable {
			columnType = append(columnType, JSONTypeNull)
		}

		columnRecord[column.Name] = JsonProperty{
			Type:        columnType,
			Description: column.GetDescription(),
		}
	}

	return columnRecord
}

// typeToJSONSchemaType converts particular service type to Json type
func typeToJSONSchemaType(service, columnType string) JSONType {
	if service == "bigquery" {
		switch columnType {
		case "STRING", "DATE", "DATETIME", "TIME", "TIMESTAMP", "GEOGRAPHY":
			return JSONTypeString
		case "INT64", "NUMERIC", "FLOAT64", "INT", "FLOAT", "BIGNUMERIC":
			return JSONTypeNumber
		case "BYTES":
			return JSONTypeArray
		case "BOOLEAN":
			return JSONTypeBoolean
		case "RECORD":
			return JSONTypeObject
		default:
			return JSONTypeString
		}
	}
	if service == "postgres" {
		switch columnType {
		case "uuid", "integer", "decimal", "smallint", "bigint", "bit", "bit varying", "numeric", "real", "double precision", "cidr", "inet", "macaddr", "serial", "bigserial", "money":
			return JSONTypeNumber
		case "varchar", "text", "character", "character varying", "date", "time", "timestamp", "interval", "point", "line", "path":
			return JSONTypeString
		case "boolean":
			return JSONTypeBoolean
		case "bytea", "integer[]", "character[]", "text[]":
			return JSONTypeArray
		default:
			return JSONTypeString
		}
	}

	return ""
}

// buildAvroFields builds the avro schema fields
func buildAvroFields(asset *v1beta2.Asset, table *v1beta2.Table) []AvroFields {
	service := asset.GetService()
	columns := table.GetColumns()
	if len(columns) == 0 {
		return nil
	}

	var fields []AvroFields
	for _, column := range columns {
		dataType := typeToAvroSchemaType(service, column.DataType)
		columnType := []AvroType{dataType}

		if column.IsNullable {
			columnType = []AvroType{dataType, AvroTypeNull}
		}

		fields = append(fields, AvroFields{
			Name: column.Name,
			Type: columnType,
		})
	}

	return fields
}

// typeToAvroSchemaType converts particular service type to avro type
func typeToAvroSchemaType(service, columnType string) AvroType {
	if service == "bigquery" {
		switch columnType {
		case "STRING", "DATE", "DATETIME", "TIME", "TIMESTAMP", "GEOGRAPHY":
			return AvroTypeString
		case "INT64", "NUMERIC", "INT", "BIGNUMERIC":
			return AvroTypeInteger
		case "FLOAT64", "FLOAT":
			return AvroTypeFloat
		case "BYTES":
			return AvroTypeBytes
		case "BOOLEAN":
			return AvroTypeBoolean
		case "RECORD":
			return AvroTypeRecord
		default:
			return AvroTypeString
		}
	}
	if service == "postgres" {
		switch columnType {
		case "uuid", "integer", "decimal", "smallint", "bigint", "bit", "bit varying", "numeric", "real", "double precision", "cidr", "inet", "macaddr", "serial", "bigserial", "money":
			return AvroTypeInteger
		case "varchar", "text", "character", "character varying", "date", "time", "timestamp", "interval", "point", "line", "path":
			return AvroTypeString
		case "boolean":
			return AvroTypeBoolean
		case "bytea", "integer[]", "character[]", "text[]":
			return AvroTypeArray
		default:
			return AvroTypeString
		}
	}

	return ""
}

// init register the sink to the catalog
func init() {
	if err := registry.Sinks.Register("stencil", func() plugins.Syncer {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
