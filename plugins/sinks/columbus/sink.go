package columbus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/pkg/errors"
)

type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

type Config struct {
	Host    string            `mapstructure:"host" validate:"required"`
	Type    string            `mapstructure:"type" validate:"required"`
	Mapping map[string]string `mapstructure:"mapping"`
}

type Sink struct {
	client httpClient
	config Config
}

func New(c httpClient) plugins.Syncer {
	sink := &Sink{client: c}
	return sink
}

func (s *Sink) Sink(ctx context.Context, configMap map[string]interface{}, in <-chan interface{}) (err error) {
	if err = utils.BuildConfig(configMap, &s.config); err != nil {
		return plugins.InvalidConfigError{Type: plugins.PluginTypeSink}
	}

	for data := range in {
		data, err = s.mapData(data)
		if err != nil {
			return errors.Wrap(err, "error mapping data")
		}
		if err = s.send(data); err != nil {
			return errors.Wrap(err, "error sending data")
		}
	}

	return
}

func (s *Sink) mapData(data interface{}) (interface{}, error) {
	// skip if mapping is not defined
	if s.config.Mapping == nil {
		return data, nil
	}

	// parse data to map for easier mapping
	result, err := s.parseToMap(data)
	if err != nil {
		return data, err
	}

	// map fields
	for newField, currField := range s.config.Mapping {
		val, err := s.getValueFromField(result, currField)
		if err != nil {
			return result, errors.Wrap(err, "error getting value from field")
		}

		result[newField] = val
	}

	return result, nil
}

func (s *Sink) parseToMap(data interface{}) (result map[string]interface{}, err error) {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return
	}
	err = json.Unmarshal(jsonBytes, &result)
	if err != nil {
		return
	}

	return result, nil
}

func (s *Sink) getValueFromField(data map[string]interface{}, fieldName string) (interface{}, error) {
	var value interface{}
	nestedFields := strings.Split(fieldName, ".") // "resource.urn" -> ["resource", "urn"]
	totalNestedLevel := len(nestedFields)         // total nested level

	temp := data
	for i := 0; i < totalNestedLevel; i++ {
		var ok bool
		field := nestedFields[i]
		value, ok = temp[field]
		if !ok {
			return value, fmt.Errorf("could not find field \"%s\"", field)
		}
		if i < totalNestedLevel-1 {
			temp, ok = value.(map[string]interface{})
			if !ok {
				return value, fmt.Errorf("field \"%s\" is not a map", field)
			}
		}
	}

	return value, nil
}

func (s *Sink) send(data interface{}) (err error) {
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
	if err := registry.Sinks.Register("columbus", func() plugins.Syncer {
		return New(&http.Client{})
	}); err != nil {
		panic(err)
	}
}
