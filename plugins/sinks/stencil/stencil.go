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
	"github.com/raystack/meteor/metrics/otelhttpclient"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/internal/urlbuilder"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
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
	Tags:        []string{"oss", "schema"},
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
	urlb   urlbuilder.Source
}

// New returns a pointer to an initialized Sink Object
func New(c httpClient, logger log.Logger) plugins.Syncer {
	if cl, ok := c.(*http.Client); ok {
		cl.Transport = otelhttpclient.NewHTTPTransport(cl.Transport)
	}

	s := &Sink{
		logger: logger,
		client: c,
	}
	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)

	return s
}

// Init initializes the sink
func (s *Sink) Init(ctx context.Context, config plugins.Config) error {
	if err := s.BasePlugin.Init(ctx, config); err != nil {
		return err
	}

	urlb, err := urlbuilder.NewSource(s.config.Host)
	if err != nil {
		return err
	}
	s.urlb = urlb

	return nil
}

// column represents a column extracted from entity properties.
type column struct {
	Name        string
	DataType    string
	IsNullable  bool
	Description string
}

// extractColumns extracts column information from entity properties.
func extractColumns(entity *meteorv1beta1.Entity) []column {
	props := entity.GetProperties()
	if props == nil {
		return nil
	}

	columnsVal, ok := props.GetFields()["columns"]
	if !ok {
		return nil
	}

	columnsList := columnsVal.GetListValue()
	if columnsList == nil {
		return nil
	}

	var columns []column
	for _, item := range columnsList.GetValues() {
		colMap := item.GetStructValue()
		if colMap == nil {
			continue
		}

		fields := colMap.GetFields()
		c := column{}
		if v, ok := fields["name"]; ok {
			c.Name = v.GetStringValue()
		}
		if v, ok := fields["data_type"]; ok {
			c.DataType = v.GetStringValue()
		}
		if v, ok := fields["is_nullable"]; ok {
			c.IsNullable = v.GetBoolValue()
		}
		if v, ok := fields["description"]; ok {
			c.Description = v.GetStringValue()
		}

		columns = append(columns, c)
	}

	return columns
}

// Sink helps to sink record to stencil
func (s *Sink) Sink(ctx context.Context, batch []models.Record) error {
	for _, record := range batch {
		entity := record.Entity()
		columns := extractColumns(entity)
		if len(columns) == 0 {
			continue
		}

		s.logger.Info("sinking record to stencil", "record", entity.GetUrn())

		var (
			payload any
			err     error
		)
		switch s.config.Format {
		case "avro":
			payload, err = s.buildAvroStencilPayload(entity, columns)
		case "json":
			payload, err = s.buildJsonStencilPayload(entity, columns)
		}

		if err != nil {
			return fmt.Errorf("build stencil payload: %w", err)
		}
		if err := s.send(ctx, entity.GetUrn(), payload); err != nil {
			return fmt.Errorf("send stencil payload: %w", err)
		}

		s.logger.Info("successfully sunk record to stencil", "record", entity.GetUrn())
	}

	return nil
}

// Close will be called once after everything is done
func (*Sink) Close() error { return nil }

// buildJsonStencilPayload build json stencil payload
func (s *Sink) buildJsonStencilPayload(entity *meteorv1beta1.Entity, columns []column) (JsonSchema, error) {
	jsonProperties := buildJsonProperties(entity, columns)

	record := JsonSchema{
		Id:         entity.GetUrn() + ".json",
		Schema:     "https://json-schema.org/draft/2020-12/schema",
		Title:      entity.GetName(),
		Type:       JSONTypeObject,
		Properties: jsonProperties,
	}

	return record, nil
}

// buildAvroStencilPayload build Json stencil payload
func (s *Sink) buildAvroStencilPayload(entity *meteorv1beta1.Entity, columns []column) (AvroSchema, error) {
	avroFields := buildAvroFields(entity, columns)

	record := AvroSchema{
		Type:      "record",
		Namespace: s.config.NamespaceID,
		Name:      entity.GetName(),
		Fields:    avroFields,
	}

	return record, nil
}

// send helps to pass data to stencil
func (s *Sink) send(ctx context.Context, tableURN string, record any) error {
	schemaID := strings.ReplaceAll(tableURN, "/", ".")

	const schemaRoute = "/v1beta1/namespaces/{namespaceID}/schemas/{schemaID}"
	targetURL := s.urlb.New().
		Path(schemaRoute).
		PathParam("namespaceID", s.config.NamespaceID).
		PathParam("schemaID", schemaID).
		URL()

	payloadBytes, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// send request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL.String(), bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}
	req = otelhttpclient.AnnotateRequest(req, schemaRoute)

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
func buildJsonProperties(entity *meteorv1beta1.Entity, columns []column) map[string]JsonProperty {
	if len(columns) == 0 {
		return nil
	}

	columnRecord := make(map[string]JsonProperty)
	source := entity.GetSource()

	for _, col := range columns {
		dataType := typeToJSONSchemaType(source, col.DataType)
		columnType := []JSONType{dataType}

		if col.IsNullable {
			columnType = append(columnType, JSONTypeNull)
		}

		columnRecord[col.Name] = JsonProperty{
			Type:        columnType,
			Description: col.Description,
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
func buildAvroFields(entity *meteorv1beta1.Entity, columns []column) []AvroFields {
	if len(columns) == 0 {
		return nil
	}

	source := entity.GetSource()

	var fields []AvroFields
	for _, col := range columns {
		dataType := typeToAvroSchemaType(source, col.DataType)
		columnType := []AvroType{dataType}

		if col.IsNullable {
			columnType = []AvroType{dataType, AvroTypeNull}
		}

		fields = append(fields, AvroFields{
			Name: col.Name,
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
