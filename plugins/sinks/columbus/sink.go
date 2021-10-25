package columbus

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
)

//go:embed README.md
var summary string

type Config struct {
	Host string `mapstructure:"host" validate:"required"`
	Type string `mapstructure:"type" validate:"required"`
}

var sampleConfig = `
# The hostnmame of the columbus service
host: https://columbus.com
# The type of the data to send
type: sample-columbus-type`

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
		Description:  "Send metadata to columbus http service",
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
		s.logger.Info("sinking record to columbus", "record", metadata.GetResource().Urn)

		columbusPayload := s.buildColumbusPayload(metadata)
		if err = s.send(columbusPayload); err != nil {
			return errors.Wrap(err, "error sending data")
		}
		s.logger.Info("successfully sinked record to columbus", "record", metadata.GetResource().Urn)
	}

	return
}

func (s *Sink) Close() (err error) { return }

func (s *Sink) send(record Record) (err error) {
	payloadBytes, err := json.Marshal([]Record{record})
	if err != nil {
		return
	}

	// send request
	url := fmt.Sprintf("%s/v1/types/%s/records", s.config.Host, s.config.Type)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return
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
	err = fmt.Errorf("columbus returns %d: %v", res.StatusCode, string(bodyBytes))

	switch code := res.StatusCode; {
	case code >= 500:
		return plugins.NewRetryError(err)
	default:
		return err
	}
}

func (s *Sink) buildColumbusPayload(metadata models.Metadata) Record {
	record := Record{
		Urn:  metadata.GetResource().Urn,
		Name: metadata.GetResource().Name,
		Data: metadata,
	}

	return record
}

func init() {
	if err := registry.Sinks.Register("columbus", func() plugins.Syncer {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
