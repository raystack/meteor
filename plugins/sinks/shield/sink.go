package shield

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/models"
	assetsv1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	sh "github.com/odpf/shield/proto/v1beta1"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
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
	client Client
	config Config
	logger log.Logger
}

func New(c Client, logger log.Logger) plugins.Syncer {
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

	if err := s.client.Connect(ctx, s.config.Host); err != nil {
		return fmt.Errorf("error connecting to host: %w", err)
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

func (s *Sink) Close() (err error) {
	return
	//TODO: Connection closes even when some records are unpiblished
	//TODO: return s.client.Close()
}

func (s *Sink) send(ctx context.Context, record RequestPayload) error {
	for hdrKey, hdrVal := range s.config.Headers {
		hdrVals := strings.Split(hdrVal, ",")
		for _, val := range hdrVals {
			val = strings.TrimSpace(val)
			md := metadata.New(map[string]string{hdrKey: val})
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
	}

	metadataBytes, err := json.Marshal(record.Metadata)
	if err != nil {
		return err
	}

	var requestBody sh.UserRequestBody
	requestBody.Name = record.Name
	requestBody.Email = record.Email
	requestBody.Metadata = new(structpb.Struct)

	err = json.Unmarshal(metadataBytes, requestBody.Metadata)
	if err != nil {
		return err
	}

	_, err = s.client.UpdateUser(ctx, &sh.UpdateUserRequest{
		Id:   requestBody.Email,
		Body: &requestBody,
	})
	if err == nil {
		return nil
	}

	if e, ok := status.FromError(err); ok {
		err = fmt.Errorf("shield returns code %d: %v", e.Code(), e.Message())
		switch e.Code() {
		case codes.Unavailable:
			return plugins.NewRetryError(err)
		default:
			return err
		}
	} else {
		err = fmt.Errorf("not able to parse error returned %v", err)
	}

	return err
}

func (s *Sink) buildShieldPayload(resource *assetsv1beta2.Asset) (RequestPayload, error) {
	data := resource.GetData()

	mapdata, err := s.buildShieldData(data)
	if err != nil {
		return RequestPayload{}, err
	}

	name, ok := mapdata["full_name"].(string)
	if !ok {
		return RequestPayload{}, errors.New(fmt.Sprintf("unexpected type %T for name, must be a string", mapdata["full_name"]))
	}

	email, ok := mapdata["email"].(string)
	if !ok {
		return RequestPayload{}, errors.New(fmt.Sprintf("unexpected type %T for email, must be a string", mapdata["email"]))
	}

	metadata, ok := mapdata["attributes"].(map[string]interface{})
	if !ok {
		return RequestPayload{}, errors.New(fmt.Sprintf("unexpected type %T for attributes, must be a map[string]interface{}", mapdata["attributes"]))
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
		return New(newClient(), plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
