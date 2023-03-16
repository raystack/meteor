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
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
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
}

func New(c httpClient, logger log.Logger) plugins.Syncer {
	s := &Sink{client: c, logger: logger}

	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)
	return s
}

func (s *Sink) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = s.BasePlugin.Init(ctx, config); err != nil {
		return err
	}

	return
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	for _, record := range batch {
		asset := record.Data()
		s.logger.Info("sinking record to compass", "record", asset.GetUrn())

		compassPayload, err := s.buildCompassPayload(asset)
		if err != nil {
			return errors.Wrap(err, "failed to build compass payload")
		}
		if err = s.send(compassPayload); err != nil {
			return errors.Wrap(err, "error sending data")
		}

		s.logger.Info("successfully sinked record to compass", "record", asset.GetUrn())
	}

	return
}

func (s *Sink) Close() (err error) { return }

func (s *Sink) send(record RequestPayload) (err error) {
	payloadBytes, err := json.Marshal(record)
	if err != nil {
		return
	}

	// send request
	url := fmt.Sprintf("%s/v1beta1/assets", s.config.Host)
	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewBuffer(payloadBytes))
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
	bodyBytes, err = io.ReadAll(res.Body)
	if err != nil {
		return
	}
	err = fmt.Errorf("compass returns %d: %v", res.StatusCode, string(bodyBytes))

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
		return RequestPayload{}, errors.Wrap(err, "failed to build labels")
	}

	mapData, err := s.buildCompassData(asset.GetData())
	if err != nil {
		return RequestPayload{}, errors.Wrap(err, "error building compass data")
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

	marshaler := &protojson.MarshalOptions{
		UseProtoNames: true,
	}
	bytes, err := marshaler.Marshal(anyData)
	if err != nil {
		return mapData, errors.Wrap(err, "error marshaling asset data")
	}

	err = json.Unmarshal(bytes, &mapData)
	if err != nil {
		return mapData, errors.Wrap(err, "error unmarshalling to mapdata")
	}

	return mapData, nil
}

func (s *Sink) buildLineage(asset *v1beta2.Asset) (upstreams, downstreams []LineageRecord) {
	lineage := asset.GetLineage()
	if lineage == nil {
		return
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

	return
}

func (s *Sink) buildOwners(asset *v1beta2.Asset) (owners []Owner) {
	for _, ownerProto := range asset.GetOwners() {
		owners = append(owners, Owner{
			URN:   ownerProto.Urn,
			Name:  ownerProto.Name,
			Role:  ownerProto.Role,
			Email: ownerProto.Email,
		})
	}
	return
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
			return nil, errors.Wrapf(err, "could not find %q", template)
		}

		labels[key] = value
	}

	return labels, nil
}

func (s *Sink) buildLabelValue(template string, asset *v1beta2.Asset) (value string, err error) {
	fields := strings.Split(template, ".")
	if len(fields) < 2 {
		err = errors.New("label template has to be at least nested 2 levels")
		return
	}

	value, err = s.getLabelValueFromProperties(fields[0], fields[1], asset)
	if err != nil {
		err = fmt.Errorf("error getting label value")
		return
	}

	return
}

func (s *Sink) getLabelValueFromProperties(field1 string, field2 string, asset *v1beta2.Asset) (value string, err error) {
	switch field1 {
	case "$attributes":
		attr := utils.GetAttributes(asset)
		v, ok := attr[field2]
		if !ok {
			err = fmt.Errorf("could not find %q field on attributes", field2)
			return
		}
		value, ok = v.(string)
		if !ok {
			err = fmt.Errorf("%q field is not a string", field2)
			return
		}
		return
	case "$labels":
		labels := asset.GetLabels()
		if labels == nil {
			err = errors.New("could not find labels field")
			return
		}
		var ok bool
		value, ok = labels[field2]
		if !ok {
			err = fmt.Errorf("could not find %q from labels", field2)
			return
		}

		return
	}

	err = errors.New("invalid label template format")
	return
}

func init() {
	if err := registry.Sinks.Register("compass", func() plugins.Syncer {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
