package frontier

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"

	sh "github.com/raystack/frontier/proto/v1beta1"
	"github.com/raystack/meteor/models"
	v1beta2 "github.com/raystack/meteor/models/raystack/assets/v1beta2"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/frontier/client"
	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/log"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed README.md
var summary string

const (
	service = "frontier"
)

// Config holds the set of configuration for the frontier extractor
type Config struct {
	Host string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `host: frontier.com:80`

var info = plugins.Info{
	Description:  "Frontier' users metadata",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"frontier", "extractor"},
}

// Extractor manages the communication with the frontier service
type Extractor struct {
	plugins.BaseExtractor
	logger log.Logger
	config Config
	client client.Client
}

func New(l log.Logger, c client.Client) *Extractor {
	e := &Extractor{
		logger: l,
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

	if err := e.client.Connect(ctx, e.config.Host); err != nil {
		return fmt.Errorf("error connecting to host: %w", err)
	}

	return
}

// Extract extracts the user information
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	defer e.client.Close()

	listUsers, err := e.client.ListUsers(ctx, &sh.ListUsersRequest{})
	if err != nil {
		return fmt.Errorf("error fetching users: %w", err)
	}

	for _, user := range listUsers.Users {
		grp, grpErr := e.client.GetGroup(ctx, &sh.GetGroupRequest{Id: user.GetId()})
		if grpErr != nil {
			return fmt.Errorf("error fetching user groups: %w", err)
		}
		data, err := anypb.New(&v1beta2.User{
			Email:    user.GetEmail(),
			Username: user.GetId(),
			FullName: user.GetName(),
			Status:   "active",
			Memberships: []*v1beta2.Membership{
				{
					GroupUrn: fmt.Sprintf("%s:%s", grp.Group.GetName(), grp.Group.GetId()),
				},
			},
			Attributes: &structpb.Struct{},
			CreateTime: user.GetCreatedAt(),
			UpdateTime: user.GetUpdatedAt(),
		})
		if err != nil {
			err = fmt.Errorf("error creating Any struct: %w", err)
			return err
		}
		emit(models.NewRecord(&v1beta2.Asset{
			Urn:     models.NewURN(service, e.UrnScope, "user", user.GetId()),
			Name:    user.GetName(),
			Service: service,
			Type:    "user",
			Data:    data,
		}))
	}

	return nil
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("frontier", func() plugins.Extractor {
		return New(plugins.GetLog(), client.New())
	}); err != nil {
		panic(err)
	}
}
