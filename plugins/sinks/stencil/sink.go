package stencil

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/odpf/meteor/models"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"strings"
)

//go:embed README.md
var summary string

type Config struct {
	Host        string            `mapstructure:"host" validate:"required"`
	NamespaceID string            `mapstructure:"namespaceId" validate:"required"`
	SchemaID    string            `mapstructure:"schemaId" validate:"required"`
	Headers     map[string]string `mapstructure:"headers"`
}

var sampleConfig = ``

type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

type Sink struct {
	client httpClient
	config Config
	logger log.Logger
}

func New(c httpClient, logger log.Logger) plugins.Syncer {
	sink := &Sink{client: c, logger: logger}
	return sink
}

func (s *Sink) Info() plugins.Info {
	return plugins.Info{
		Description:  "Send metadata to stencil http service",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"http", "sink"},
	}
}

func (s *Sink) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (s *Sink) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	if err = utils.BuildConfig(configMap, &s.config); err != nil {
		return plugins.InvalidConfigError{Type: plugins.PluginTypeSink}
	}

	return
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {

	for _, record := range batch {
		metadata := record.Data()

		table, ok := metadata.(*assetsv1beta1.Table)
		if !ok {
			continue
		}
		s.logger.Info("sinking record to stencil", "record", table.GetResource().Urn)

		stencilPayload, err := s.buildJsonStencilPayload(table)
		if err != nil {
			return errors.Wrap(err, "failed to build stencil payload")
		}
		if err = s.send(stencilPayload); err != nil {
			return errors.Wrap(err, "error sending data")
		}

		s.logger.Info("successfully sinked record to stencil", "record", table.GetResource().Urn)
	}

	return
}

func (s *Sink) Close() (err error) { return }

func (s *Sink) send(record JsonSchema) (err error) {

	// for json schema format
	payloadBytes, err := json.Marshal(record)
	if err != nil {
		return
	}

	// send request
	url := fmt.Sprintf("%s/v1beta1/namespaces/%s/schemas/%s", s.config.Host, s.config.NamespaceID, s.config.SchemaID)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return
	}

	for hdrKey, hdrVal := range s.config.Headers {
		hdrVals := strings.Split(hdrVal, ",")
		for _, val := range hdrVals {
			req.Header.Add(hdrKey, val)
		}
	}

	res, err := s.client.Do(req)
	if err != nil {
		return
	}
	if res.StatusCode == 200 {
		return
	}

	var bodyBytes []byte
	bodyBytes, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	err = fmt.Errorf("stencil returns %d: %v", res.StatusCode, string(bodyBytes))

	switch code := res.StatusCode; {
	case code >= 500:
		return plugins.NewRetryError(err)
	default:
		return err
	}
}

func (s *Sink) buildJsonStencilPayload(table *assetsv1beta1.Table) (JsonSchema, error) {
	resource := table.GetResource()
	properties := s.buildJsonProperties(table)

	record := JsonSchema{
		Id:         fmt.Sprintf("%s/%s.%s.json", s.config.Host, s.config.NamespaceID, s.config.SchemaID),
		Schema:     "https://json-schema.org/draft/2020-12/schema",
		Title:      resource.GetName(),
		Type:       JsonType(resource.GetType()),
		Properties: properties,
	}

	return record, nil
}

func (s *Sink) buildJsonProperties(table *assetsv1beta1.Table) map[string]Property {
	fmt.Println("aya kya")
	columns := table.GetSchema().GetColumns()
	if columns == nil {
		fmt.Println("idhr aya kya")
		return nil
	}
	columnRecord := make(map[string]Property)

	for _, column := range columns {
		fmt.Println("column m aya kya")
		dataType := s.typeToJsonSchemaType(table, column)
		fmt.Println("datatype", dataType)
		columnType := []JsonType{dataType}

		if column.IsNullable {
			columnType = []JsonType{dataType, JsonTypeNull}
		}

		columnRecord[column.Name] = Property{
			Type:        columnType,
			Description: column.GetDescription(),
		}
		fmt.Println("col record", columnRecord)

	}
	fmt.Println("col record", columnRecord)

	return columnRecord
}

func (s *Sink) typeToJsonSchemaType(table *assetsv1beta1.Table, column *facetsv1beta1.Column) (dataType JsonType) {
	fmt.Println("schema m aya kya")
	service := table.GetResource().GetService()

	if service == "bigquery" {
		fmt.Println("bigquery m aya kya")
		switch column.DataType {
		case "STRING", "DATE", "DATETIME", "TIME", "TIMESTAMP", "GEOGRAPHY":
			dataType = JsonTypeString
		case "INT64", "NUMERIC", "FLOAT64", "INT", "FLOAT", "BIGNUMERIC":
			dataType = JsonTypeNumber
		case "BYTES":
			dataType = JsonTypeArray
		case "BOOLEAN":
			dataType = JsonTypeBoolean
		case "RECORD":
			dataType = JsonTypeObject
		default:
			dataType = JsonTypeString
		}
	}
	if service == "postgres" {
		switch column.DataType {
		case "uuid", "integer", "decimal", "smallint", "bigint", "bit", "bit varying", "numeric", "real", "double precision", "cidr", "inet", "macaddr", "serial", "bigserial", "money":
			dataType = JsonTypeNumber
		case "varchar", "text", "character", "character varying", "date", "time", "timestamp", "interval", "point", "line", "path":
			dataType = JsonTypeString
		case "boolean":
			dataType = JsonTypeBoolean
		case "bytea", "integer[]", "character[]", "text[]":
			dataType = JsonTypeArray
		default:
			dataType = JsonTypeString
		}
	}
	fmt.Println("schema se gya kya")

	return
}

func init() {
	if err := registry.Sinks.Register("stencil", func() plugins.Syncer {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
