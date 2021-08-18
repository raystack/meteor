package columbus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
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
	client           httpClient
	cachedDataMapper func(interface{}) interface{}
	config           Config
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
		dataMapper := s.getDataMapper(data)
		data = dataMapper(data)
		if err = s.send(data); err != nil {
			return
		}
	}

	return
}

func (s *Sink) getDataMapper(data interface{}) func(interface{}) interface{} {
	if s.cachedDataMapper == nil {
		s.cachedDataMapper = s.buildDataMapper(data)
	}

	return s.cachedDataMapper
}

func (s *Sink) buildDataMapper(data interface{}) func(interface{}) interface{} {
	if s.config.Mapping == nil {
		return s.defaultMapper
	}

	// build new type
	value := reflect.ValueOf(data)
	newType := s.buildNewType(value, s.config.Mapping)

	return func(d interface{}) interface{} {
		v := reflect.ValueOf(d)
		newValue := v.Convert(newType)
		return newValue.Interface()
	}
}

func (s *Sink) buildNewType(value reflect.Value, mapping map[string]string) reflect.Type {
	t := value.Type()
	sf := make([]reflect.StructField, 0)
	for i := 0; i < t.NumField(); i++ {
		sf = append(sf, t.Field(i))

		// rename json tag if field is included in mapping
		if jsonField, ok := mapping[t.Field(i).Name]; ok {
			sf[i].Tag = reflect.StructTag(fmt.Sprintf(`json:"%s"`, jsonField))
		}
	}

	return reflect.StructOf(sf)
}

func (s *Sink) defaultMapper(data interface{}) interface{} { return data }

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
