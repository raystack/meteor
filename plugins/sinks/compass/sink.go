package compass

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
	"github.com/goto/meteor/metrics/otelhttpclient"
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/internal/urlbuilder"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/utils"
	"github.com/goto/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

//go:embed README.md
var summary string

type Config struct {
	Host    string            `mapstructure:"host" validate:"required"`
	Headers map[string]string `mapstructure:"headers"`
	Labels  map[string]string `mapstructure:"labels"`
	// RemoveUnsetFieldsInData if set to true do not populate fields in final sink data which are unset in initial data.
	RemoveUnsetFieldsInData bool `mapstructure:"remove_unset_fields_in_data"`
}

var info = plugins.Info{
	Description: "Send metadata to compass http service",
	Summary:     summary,
	Tags:        []string{"http", "sink"},
	SampleConfig: heredoc.Doc(`
	# The hostname of the compass service
	host: https://compass.com
	# Additional HTTP headers send to compass, multiple headers value are separated by a comma
	headers:
	  Compass-User-Email: meteor@gotocompany.com
	  X-Other-Header: value1, value2
	# The labels to pass as payload label of the patch api
	labels:
	  myCustom: $attributes.myCustomField
	  sampleLabel: $labels.sampleLabelField
	`),
}

type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

type Sink struct {
	plugins.BasePlugin
	client httpClient
	config Config
	logger log.Logger
	urlb   urlbuilder.Source
}

func New(c httpClient, logger log.Logger) plugins.Syncer {
	if cl, ok := c.(*http.Client); ok {
		cl.Transport = otelhttpclient.NewHTTPTransport(cl.Transport)
	}

	s := &Sink{client: c, logger: logger}

	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)
	return s
}

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

func (s *Sink) Sink(ctx context.Context, batch []models.Record) error {
	for _, record := range batch {
		asset := record.Data()
		s.logger.Info("sinking record to compass", "record", asset.GetUrn())

		compassPayload, err := s.buildCompassPayload(asset)
		if err != nil {
			return fmt.Errorf("build compass payload: %w", err)
		}
		if err = s.send(ctx, compassPayload); err != nil {
			return fmt.Errorf("send data: %w", err)
		}

		s.logger.Info("successfully sinked record to compass", "record", asset.GetUrn())
	}

	return nil
}

func (*Sink) Close() error { return nil }

func (s *Sink) send(ctx context.Context, record RequestPayload) error {
	const assetsRoute = "/v1beta1/assets"
	targetURL := s.urlb.New().Path(assetsRoute).URL()

	payloadBytes, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// send request
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, targetURL.String(), bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}
	req = otelhttpclient.AnnotateRequest(req, assetsRoute)

	for hdrKey, hdrVal := range s.config.Headers {
		hdrVals := strings.Split(hdrVal, ",")
		for _, val := range hdrVals {
			req.Header.Add(hdrKey, val)
		}
	}

	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer plugins.DrainBody(res)

	if res.StatusCode == 200 {
		return nil
	}

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	err = fmt.Errorf("compass returns %d: %v", res.StatusCode, string(data))
	switch code := res.StatusCode; {
	case code >= 500:
		return plugins.NewRetryError(err)
	default:
		return err
	}
}

func (s *Sink) buildCompassPayload(asset *v1beta2.Asset) (RequestPayload, error) {
	labels, err := s.buildLabels(asset)
	if err != nil {
		return RequestPayload{}, fmt.Errorf("build labels: %w", err)
	}

	mapData, err := s.buildCompassData(asset.GetData())
	if err != nil {
		return RequestPayload{}, fmt.Errorf("build compass data: %w", err)
	}

	upstreams, downstreams := s.buildLineage(asset)
	owners := s.buildOwners(asset)
	record := RequestPayload{
		Asset: Asset{
			URN:         asset.GetUrn(),
			Type:        asset.GetType(),
			Name:        asset.GetName(),
			Service:     asset.GetService(),
			Description: asset.GetDescription(),
			URL:         asset.GetUrl(),
			Owners:      owners,
			Data:        mapData,
			Labels:      labels,
		},
		Upstreams:   upstreams,
		Downstreams: downstreams,
	}

	return record, nil
}

func (s *Sink) buildCompassData(anyData *anypb.Any) (map[string]interface{}, error) {
	var mapData map[string]interface{}

	data, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: !s.config.RemoveUnsetFieldsInData,
	}.Marshal(anyData)
	if err != nil {
		return nil, fmt.Errorf("marshaling asset data: %w", err)
	}

	if err := json.Unmarshal(data, &mapData); err != nil {
		return nil, fmt.Errorf("unmarshalling to mapdata: %w", err)
	}

	return mapData, nil
}

func (s *Sink) buildLineage(asset *v1beta2.Asset) (upstreams, downstreams []LineageRecord) {
	lineage := asset.GetLineage()
	if lineage == nil {
		return nil, nil
	}

	for _, upstream := range lineage.Upstreams {
		upstreams = append(upstreams, LineageRecord{
			URN:     upstream.Urn,
			Type:    upstream.Type,
			Service: upstream.Service,
		})
	}
	for _, downstream := range lineage.Downstreams {
		downstreams = append(downstreams, LineageRecord{
			URN:     downstream.Urn,
			Type:    downstream.Type,
			Service: downstream.Service,
		})
	}

	return upstreams, downstreams
}

func (*Sink) buildOwners(asset *v1beta2.Asset) []Owner {
	var owners []Owner
	for _, ownerProto := range asset.GetOwners() {
		owners = append(owners, Owner{
			URN:   ownerProto.Urn,
			Name:  ownerProto.Name,
			Role:  ownerProto.Role,
			Email: ownerProto.Email,
		})
	}
	return owners
}

func (s *Sink) buildLabels(asset *v1beta2.Asset) (map[string]string, error) {
	total := len(s.config.Labels) + len(asset.Labels)
	if total == 0 {
		return nil, nil
	}

	labels := make(map[string]string, total)
	for k, v := range asset.Labels {
		labels[k] = v
	}

	for key, template := range s.config.Labels {
		value, err := s.buildLabelValue(template, asset)
		if err != nil {
			return nil, fmt.Errorf("find %q: %w", template, err)
		}

		labels[key] = value
	}

	return labels, nil
}

func (s *Sink) buildLabelValue(template string, asset *v1beta2.Asset) (string, error) {
	fields := strings.Split(template, ".")
	if len(fields) < 2 {
		return "", errors.New("label template has to be at least nested 2 levels")
	}

	value, err := s.getLabelValueFromProperties(fields[0], fields[1], asset)
	if err != nil {
		return "", fmt.Errorf("get label value: %w", err)
	}

	return value, nil
}

func (*Sink) getLabelValueFromProperties(field1, field2 string, asset *v1beta2.Asset) (string, error) {
	switch field1 {
	case "$attributes":
		attr := utils.GetAttributes(asset)
		v, ok := attr[field2]
		if !ok {
			return "", fmt.Errorf("find %q field on attributes", field2)
		}
		value, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("%q field is not a string", field2)
		}
		return value, nil
	case "$labels":
		labels := asset.GetLabels()
		if labels == nil {
			return "", errors.New("find labels field")
		}

		value, ok := labels[field2]
		if !ok {
			return "", fmt.Errorf("find %q from labels", field2)
		}

		return value, nil
	}

	return "", errors.New("invalid label template format")
}

func init() {
	if err := registry.Sinks.Register("compass", func() plugins.Syncer {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
