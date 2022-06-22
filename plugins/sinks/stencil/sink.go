package stencil

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/hamba/avro"
	"github.com/odpf/meteor/models"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/sinks/stencil/pb/github.com/odpf/meteor/plugins/sinks/stencil"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
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
	Format      string            `mapstructure:"format"`
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

func (s *Sink) selectFormat(table *assetsv1beta1.Table) (JsonSchema, AvroSchema, stencil.ProtoSchema, error) {
	switch s.config.Format {
	case "avro":
		record, err := s.buildAvroStencilPayload(table)
		return JsonSchema{}, record, stencil.ProtoSchema{}, err
	case "protobuf":
		record, err := s.buildProtoStencilPayload(table)
		return JsonSchema{}, AvroSchema{}, record, err
	default:
		record, err := s.buildJsonStencilPayload(table)
		return record, AvroSchema{}, stencil.ProtoSchema{}, err
	}
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
	//record = s.SelectFormat()
	dependentSchema :=
		`{
			"type": "table",
			"name": "simple",
			"namespace": "org.hamba.avro",
			"fields" : [
				{"name": "title", "type": "long"},
				{"name": "urn", "type": "long"},
				{"name": "service", "type": "long"},
				{"name": "description", "type": "long"}
				{"name": "columns", "type": {
					type: "record",
					name: "columns",
					fields: [
						{name: "profile", type: "string"},
						{name: "name", type: "string"},
						{name: "properties", type: "string"},
						{name: "description", type: "string"},
						{name: "length", type: "int64"},
						{name: "is_nullable", type: "boolean"},
						{name: "datatype", type: "string"}
					]
				}}
			]
		}`

	// for avro schema
	parse, err := avro.Parse(dependentSchema)
	if err != nil {
		return err
	}

	marshal, err := avro.Marshal(parse, record)
	if err != nil {
		return err
	}

	// for json schema
	payloadBytes, err := json.Marshal(record)
	if err != nil {
		return
	}

	//for proto
	protoData := &stencil.ProtoSchema{}
	protoByte, err := proto.Marshal(protoData)
	if err != nil {
		return err
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
	columns := s.buildJsonColumns(table)

	record := JsonSchema{
		Id:          fmt.Sprintf("%s/%s.%s.json", s.config.Host, s.config.NamespaceID, s.config.SchemaID),
		Schema:      "https://json-schema.org/draft/2020-12/schema",
		Title:       resource.GetName(),
		Type:        resource.GetType(),
		Columns:     columns,
		Urn:         resource.GetUrn(),
		Service:     resource.GetService(),
		Description: resource.GetDescription(),
	}

	return record, nil
}

func (s *Sink) buildJsonColumns(table *assetsv1beta1.Table) (columns []Columns) {
	schema := table.GetSchema()
	if schema == nil {
		return
	}

	for _, column := range schema.GetColumns() {
		columns = append(columns, Columns{
			Profile:     column.Profile.String(),
			Name:        column.Name,
			Properties:  column.Properties.String(),
			Description: column.Description,
			Length:      column.Length,
			IsNullable:  column.IsNullable,
			DataType:    column.DataType,
		})
	}
	return
}

func (s *Sink) buildAvroStencilPayload(table *assetsv1beta1.Table) (AvroSchema, error) {
	resource := table.GetResource()
	columns := s.buildAvroColumns(table)

	record := AvroSchema{
		Title:       resource.GetName(),
		Columns:     columns,
		Urn:         resource.GetUrn(),
		Service:     resource.GetService(),
		Description: resource.GetDescription(),
	}

	return record, nil
}

func (s *Sink) buildAvroColumns(table *assetsv1beta1.Table) (columns []AvroColumns) {
	schema := table.GetSchema()
	if schema == nil {
		return
	}

	for _, column := range schema.GetColumns() {
		columns = append(columns, AvroColumns{
			Profile:     column.Profile.String(),
			Name:        column.Name,
			Properties:  column.Properties.String(),
			Description: column.Description,
			Length:      column.Length,
			IsNullable:  column.IsNullable,
			DataType:    column.DataType,
		})
	}
	return
}

func (s *Sink) buildProtoStencilPayload(table *assetsv1beta1.Table) (stencil.ProtoSchema, error) {
	resource := table.GetResource()
	columns := s.buildProtoColumns(table)

	record := stencil.ProtoSchema{
		Title:       resource.GetName(),
		Columns:     columns,
		Urn:         resource.GetUrn(),
		Service:     resource.GetService(),
		Description: resource.GetDescription(),
	}

	return record, nil
}

func (s *Sink) buildProtoColumns(table *assetsv1beta1.Table) (columns []*stencil.Columns) {
	schema := table.GetSchema()
	if schema == nil {
		return
	}

	for _, column := range schema.GetColumns() {
		columns = append(columns, &stencil.Columns{
			Profile:     column.Profile.String(),
			Name:        column.Name,
			Properties:  column.Properties.String(),
			Description: column.Description,
			Length:      column.Length,
			IsNullable:  column.IsNullable,
			DataType:    column.DataType,
		})
	}
	return
}

func init() {
	if err := registry.Sinks.Register("stencil", func() plugins.Syncer {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
