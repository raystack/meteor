package columbus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/core/sink"
	"github.com/odpf/meteor/utils"
)

type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

type Config struct {
	Host string `mapstructure:"host" validate:"required"`
	Type string `mapstructure:"type" validate:"required"`
}

type Sink struct {
	config Config
	client httpClient
}

func New(c httpClient) core.Syncer {
	sink := &Sink{client: c}
	return sink
}

func (s *Sink) Sink(ctx context.Context, configMap map[string]interface{}, in <-chan interface{}) (err error) {
	if err = utils.BuildConfig(configMap, &s.config); err != nil {
		return sink.InvalidConfigError{}
	}

	for data := range in {
		if err = s.sink(data); err != nil {
			return
		}
	}

	return
}

func (s *Sink) sink(data interface{}) (err error) {
	payload, err := s.buildPayload(data)
	if err != nil {
		return
	}

	// send request
	url := fmt.Sprintf("%s/v1/types/%s/records", s.config.Host, s.config.Type)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	if err != nil {
		return
	}
	res, err := s.client.Do(req)
	if err != nil {
		return
	}

	// build error on non-200 response
	if res.StatusCode != 200 {
		var bodyBytes []byte
		bodyBytes, err = ioutil.ReadAll(res.Body)
		if err != nil {
			return
		}

		err = fmt.Errorf("columbus returns %d: %v", res.StatusCode, string(bodyBytes))
	}

	return
}

func (s *Sink) buildPayload(data interface{}) (payload []byte, err error) {
	// wrap metadata in an array
	columbusPayload := []interface{}{data}
	return json.Marshal(columbusPayload)
}

func init() {
	if err := sink.Catalog.Register("columbus", func() core.Syncer {
		return New(&http.Client{})
	}); err != nil {
		panic(err)
	}
}
