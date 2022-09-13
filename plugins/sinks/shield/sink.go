package shield

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/models"
	assetsv1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

//go:embed README.md
var summary string

type Config struct {
	Host    string            `mapstructure:"host" validate:"required"`
	Headers map[string]string `mapstructure:"headers"`
}

var info = plugins.Info{
	Description: "Send user information to shield http service",
	Summary:     summary,
	Tags:        []string{"http", "sink"},
	SampleConfig: heredoc.Doc(`
	# The hostname of the shield service
	host: https://shield.com
	# Additional HTTP headers send to shield, multiple headers value are separated by a comma
	headers:
	  X-Shield-Email: meteor@odpf.io
      X-Other-Header: value1, value2
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
	s := &Sink{
		logger: logger,
		client: c,
	}
	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)

	return s
}

func (s *Sink) Init(ctx context.Context, config plugins.Config) error {
	if err := s.BasePlugin.Init(ctx, config); err != nil {
		return err
	}

	return nil
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) error {
	for _, record := range batch {
		metadata := record.Data()
		s.logger.Info("sinking record to shield", "record", metadata.GetUrn())

		shieldPayload, err := s.buildShieldPayload(metadata)
		if err != nil {
			return errors.Wrap(err, "failed to build shield payload")
		}
		if err = s.send(ctx, shieldPayload); err != nil {
			return errors.Wrap(err, "error sending data")
		}

		s.logger.Info("successfully sinked record to shield", "record", metadata.Name)
	}

	return nil
}

func (s *Sink) Close() (err error) { return }

func (s *Sink) send(ctx context.Context, record RequestPayload) error {
	payloadBytes, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// send request
	url := fmt.Sprintf("%s/admin/v1beta1/users/%s", url.PathEscape(s.config.Host), url.PathEscape(record.Email))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}

	for hdrKey, hdrVal := range s.config.Headers {
		hdrVals := strings.Split(hdrVal, ",")
		for _, val := range hdrVals {
			val = strings.TrimSpace(val)
			req.Header.Add(hdrKey, val)
		}
	}

	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode == 200 {
		return err
	}

	var bodyBytes []byte
	bodyBytes, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	err = fmt.Errorf("shield returns %d: %v", res.StatusCode, string(bodyBytes))

	switch code := res.StatusCode; {
	case code >= 500:
		return plugins.NewRetryError(err)
	default:
		return err
	}
}

func (s *Sink) buildShieldPayload(resource *assetsv1beta2.Asset) (RequestPayload, error) {
	data := resource.GetData()

	mapdata, err := s.buildShieldData(data)
	if err != nil {
		return RequestPayload{}, err
	}

	name, ok := mapdata["full_name"].(string)
	if !ok {
		return RequestPayload{}, errors.New("name must be a string")
	}

	email, ok := mapdata["email"].(string)
	if !ok {
		return RequestPayload{}, errors.New("email must be a string")
	}

	metadata, ok := mapdata["attributes"].(map[string]interface{})
	if !ok {
		return RequestPayload{}, errors.New("attributes must be a map[string]interface{}")
	}

	record := RequestPayload{
		Name:     name,
		Email:    email,
		Metadata: metadata,
	}

	return record, nil
}

func (s *Sink) buildShieldData(anyData *anypb.Any) (map[string]interface{}, error) {
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

func init() {
	if err := registry.Sinks.Register("shield", func() plugins.Syncer {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
