package shield

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/models"
	assetsv1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	sh "github.com/goto/shield/proto/v1beta1"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

//go:embed README.md
var summary string

type Config struct {
	Host    string            `mapstructure:"host" validate:"required"`
	Headers map[string]string `mapstructure:"headers"`
}

var info = plugins.Info{
	Description: "Send user information to shield grpc service",
	Summary:     summary,
	Tags:        []string{"grpc", "sink"},
	SampleConfig: heredoc.Doc(`
	# The hostname of the shield service
	host: shield.com:5556
	# Additional headers send to shield, multiple headers value are separated by a comma
	headers:
	  X-Shield-Email: meteor@gotocompany.com
      X-Other-Header: value1, value2
	`),
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
		asset := record.Data()
		s.logger.Info("sinking record to shield", "record", asset.GetUrn())

		userRequestBody, err := s.buildUserRequestBody(asset)
		if err != nil {
			s.logger.Error("failed to build shield payload", "err", err, "record", asset.Name)
			continue
		}

		if err = s.send(ctx, userRequestBody); err != nil {
			return fmt.Errorf("send data: %w", err)
		}

		s.logger.Info("successfully sinked record to shield", "record", asset.Name)
	}

	return nil
}

func (*Sink) Close() error {
	return nil
}

func (s *Sink) send(ctx context.Context, userRequestBody *sh.UserRequestBody) error {
	for hdrKey, hdrVal := range s.config.Headers {
		hdrVals := strings.Split(hdrVal, ",")
		for _, val := range hdrVals {
			val = strings.TrimSpace(val)
			md := metadata.New(map[string]string{hdrKey: val})
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
	}

	_, err := s.client.UpdateUser(ctx, &sh.UpdateUserRequest{
		Id:   userRequestBody.Email,
		Body: userRequestBody,
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
		err = fmt.Errorf("unable to parse error returned: %w", err)
	}

	return err
}

func (s *Sink) buildUserRequestBody(asset *assetsv1beta2.Asset) (*sh.UserRequestBody, error) {
	data := asset.GetData()

	var user assetsv1beta2.User
	err := data.UnmarshalTo(&user)
	if err != nil {
		return &sh.UserRequestBody{}, fmt.Errorf("not a User struct: %w", err)
	}

	if user.FullName == "" {
		return &sh.UserRequestBody{}, errors.New("empty user name")
	}
	if user.Email == "" {
		return &sh.UserRequestBody{}, errors.New("empty user email")
	}
	if user.Attributes == nil {
		return &sh.UserRequestBody{}, errors.New("empty user attributes")
	}

	requestBody := &sh.UserRequestBody{
		Name:     user.FullName,
		Email:    user.Email,
		Metadata: user.Attributes,
	}

	return requestBody, nil
}

func init() {
	if err := registry.Sinks.Register("shield", func() plugins.Syncer {
		return New(newClient(), plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
