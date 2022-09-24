package github

import (
	"bytes"
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/oliveagle/jsonpath"

	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
)

//go:embed README.md
var summary string

const CONST string = "CONST"

type Request struct {
	URL         string                 `mapstructure:"url" validate:"required"`
	Path        string                 `mapstructure:"path"`
	Headers     map[string]string      `mapstructure:"headers"`
	Method      string                 `mapstructure:"method" validate:"required"`
	Query       map[string]string      `mapstructure:"query"`
	Body        map[string]interface{} `mapstructure:"body"`
	ContentType string                 `mapstructure:"content_type" default:"application/json"`
	Accept      string                 `mapstructure:"accept" default:"application/json"`
}

type Mappings struct {
	Urn     string                 `mapstructure:"urn" validate:"required"`
	Name    string                 `mapstructure:"name" validate:"required"`
	Service string                 `mapstructure:"service" validate:"required"`
	Type    string                 `mapstructure:"type" validate:"required"`
	Data    map[string]interface{} `mapstructure:"data" validate:"required"`
}

type Response struct {
	Root    string   `mapstructure:"root" validate:"required"`
	Type    string   `mapstructure:"type" validate:"required"`
	Mapping Mappings `mapstructure:"mapping" validate:"required"`
}

// Config holds the set of configuration for the extractor
type Config struct {
	Request  Request  `mapstructure:"request" validate:"required"`
	Response Response `mapstructure:"response" validate:"required"`
}

var info = plugins.Info{
	Description: "Extract metadata from http service",
	Summary:     summary,
	Tags:        []string{"http", "extractor"},
}

type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	plugins.BaseExtractor
	logger log.Logger
	config Config
	client httpClient
	emit   plugins.Emit
}

// New returns a pointer to an initialized Extractor Object
func New(c httpClient, logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
		client: c,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	return
}

// Extract extracts the data from the extractor
// The data is returned as a list of assets.Asset
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	e.emit = emit

	req, err := e.buildHTTPRequest(ctx)
	if err != nil {
		return fmt.Errorf("failed to build HTTP request: %v", err)
	}

	res, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to extract: %v", err)
	}

	if res.StatusCode == 200 {
		var bodyBytes []byte
		bodyBytes, err = ioutil.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("failed to extract body: %v", err)
		}

		var s interface{}
		err := json.Unmarshal(bodyBytes, &s)
		if err != nil {
			return fmt.Errorf("failed to unmarshal: %v", err)
		}

		switch e.config.Response.Mapping.Type {
		case "user":
			return e.emitUserAsset(s)
		}
		return nil
	}

	return fmt.Errorf("request failed with status: %v", res.StatusCode)
}

func (e *Extractor) emitUserAsset(i interface{}) error {
	if e.config.Response.Type != "list" {
		u, err := jsonpath.JsonPathLookup(i, fmt.Sprintf("$.%s", e.config.Response.Root))
		if err != nil {
			return err
		}

		asset, err := e.buildUserAsset(u)
		if err != nil {
			return err
		} else if asset == nil {
			return nil
		}

		e.emit(models.NewRecord(asset))

		return nil
	}

	idx := 0
	for {
		u, err := jsonpath.JsonPathLookup(i, fmt.Sprintf("$.%s[%d]", e.config.Response.Root, idx))
		if err != nil {
			if strings.Contains(err.Error(), "index out of range") {
				break
			}
			return err
		}
		idx++

		asset, err := e.buildUserAsset(u)
		if err != nil {
			return err
		} else if asset == nil {
			continue
		}

		e.emit(models.NewRecord(asset))
	}

	return nil
}

func (e *Extractor) buildUserAsset(u interface{}) (*v1beta2.Asset, error) {
	email := e.keyJsonPathLookup(u, e.config.Response.Mapping.Data["email"].(string), "email")
	if email == "" {
		return nil, nil
	}

	fullname := e.keyJsonPathLookup(u, e.config.Response.Mapping.Data["fullname"].(string), "fullname")
	if fullname == "" {
		return nil, nil
	}

	status := e.keyJsonPathLookup(u, e.config.Response.Mapping.Data["status"].(string), "status")
	if status == "" {
		return nil, nil
	}

	attributesMap := e.config.Response.Mapping.Data["attributes"].(map[string]interface{})

	attributes := make(map[string]interface{})
	for key, value := range attributesMap {
		val := e.keyJsonPathLookup(u, value.(string), key)
		if status == "" {
			return nil, nil
		}

		attributes[key] = val
	}

	assetUser, err := anypb.New(&v1beta2.User{
		Email:      email,
		FullName:   fullname,
		Status:     status,
		Attributes: utils.TryParseMapToProto(attributes),
	})
	if err != nil {
		return nil, fmt.Errorf("error when creating anypb.Any: %w", err)
	}

	urn := e.keyJsonPathLookup(u, e.config.Response.Mapping.Urn, "urn")
	if urn == "" {
		return nil, nil
	}

	name := e.keyJsonPathLookup(u, e.config.Response.Mapping.Name, "name")
	if name == "" {
		return nil, nil
	}

	asset := &v1beta2.Asset{
		Urn:     models.NewURN("http", e.UrnScope, "user", urn),
		Name:    name,
		Service: e.config.Response.Mapping.Service,
		Type:    "user",
		Data:    assetUser,
	}

	return asset, nil
}

func (e *Extractor) keyJsonPathLookup(u interface{}, key, lable string) string {
	s := strings.Split(key, `.`)
	if s[0] == CONST && len(s) == 2 {
		key = s[1]
	}

	v, err := jsonpath.JsonPathLookup(u, fmt.Sprintf("$.%s", s[1]))
	if err != nil {
		e.logger.Error("can't find %s : %v", lable, err)
		return ""
	}

	value, ok := v.(string)
	if !ok {
		e.logger.Error("%v is not a string", v)
		return ""
	}
	return value
}

func (e *Extractor) buildHTTPRequest(ctx context.Context) (*http.Request, error) {
	var URL string

	if e.config.Request.Path != "" {
		URL = e.config.Request.URL + "/" + e.config.Request.Path
	}

	params := url.Values{}
	if e.config.Request.Query != nil {
		for param, value := range e.config.Request.Query {
			values := strings.Split(value, ",")

			valueArr := []string{}
			valueArr = append(valueArr, values...)
			params[param] = valueArr
		}
		URL = URL + "?" + params.Encode()
	}

	payloadBytes, err := json.Marshal(e.config.Request.Body)
	if err != nil {
		e.logger.Error("Unable to marshal body", err)
	}

	req, err := http.NewRequestWithContext(ctx, e.config.Request.Method, URL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, err
	}

	for hdrKey, hdrVal := range e.config.Request.Headers {
		hdrVals := strings.Split(hdrVal, ",")
		for _, val := range hdrVals {
			req.Header.Add(hdrKey, val)
		}
	}

	req.Header.Add("Content-Type", e.config.Request.ContentType)
	req.Header.Add("Accept", e.config.Request.Accept)

	return req, nil
}

// init registers the extractor to catalog
func init() {
	if err := registry.Extractors.Register("http", func() plugins.Extractor {
		return New(&http.Client{}, plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
