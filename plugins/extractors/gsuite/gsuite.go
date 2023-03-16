package gsuite

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"reflect"
	"strings"

	"github.com/goto/meteor/models"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/utils"
	"github.com/goto/salt/log"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/types/known/anypb"

	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
)

//go:embed README.md
var summary string

type Config struct {
	ServiceAccountJSON string `mapstructure:"service_account_json" validate:"required"`
	UserEmail          string `mapstructure:"user_email" validate:"required"`
}

var sampleConfig = `
service_account_json: {
    "type": "service_account",
    "project_id": "XXXXXX",
    "private_key_id": "XXXXXX",
    "private_key": "XXXXXX",
    "client_email": "XXXXXX",
    "client_id": "XXXXXX",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "XXXXXX"
}
user_email: user@gotocompany.com`

var info = plugins.Info{
	Description:  "User list from Google Workspace",
	SampleConfig: sampleConfig,
	Tags:         []string{"platform", "extractor"},
	Summary:      summary,
}

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	plugins.BaseExtractor
	logger             log.Logger
	config             Config
	userServiceFactory UsersServiceFactory
	userService        UsersListCall
	emit               plugins.Emit
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger, userServiceFactory UsersServiceFactory) *Extractor {
	e := &Extractor{
		logger:             logger,
		userServiceFactory: userServiceFactory,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	e.userService, err = e.userServiceFactory.BuildUserService(ctx, e.config.UserEmail, e.config.ServiceAccountJSON)
	if err != nil {
		return fmt.Errorf("error building user service: %w", err)
	}

	return
}

// Extract extracts the data from the extractor
// The data is returned as a list of assets.Asset
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	e.emit = emit
	adminUsers, err := e.fetchUsers(ctx)
	if err != nil {
		return err
	}

	if len(adminUsers.Users) == 0 {
		e.logger.Info("No users found.\n")
		return nil
	}

	for _, u := range adminUsers.Users {
		asset, err := e.buildAsset(u)
		if err != nil {
			e.logger.Warn("error when building asset", "err", err)
			continue
		}
		e.emit(models.NewRecord(asset))
	}

	return nil
}

func (e *Extractor) buildAsset(gsuiteUser *admin.User) (*v1beta2.Asset, error) {
	var status string
	if gsuiteUser.Suspended {
		status = "suspended"
	}

	var userAttributes = make(map[string]interface{})
	userAttributes["organizations"] = e.buildMapFromGsuiteSlice(gsuiteUser.Organizations)
	userAttributes["relations"] = e.buildMapFromGsuiteSlice(gsuiteUser.Relations)
	userAttributes["custom_schemas"] = e.buildMapFromGsuiteMapRawMessage(gsuiteUser.CustomSchemas)
	userAttributes["aliases"] = strings.Join(gsuiteUser.Aliases, ",")
	userAttributes["org_unit_path"] = gsuiteUser.OrgUnitPath

	assetUser, err := anypb.New(&v1beta2.User{
		Email:      gsuiteUser.PrimaryEmail,
		FullName:   gsuiteUser.Name.FullName,
		Status:     status,
		Attributes: utils.TryParseMapToProto(userAttributes),
	})
	if err != nil {
		return nil, fmt.Errorf("error when creating anypb.Any: %w", err)
	}

	asset := &v1beta2.Asset{
		Urn:     models.NewURN("gsuite", e.UrnScope, "user", gsuiteUser.PrimaryEmail),
		Name:    gsuiteUser.Name.FullName,
		Service: "gsuite",
		Type:    "user",
		Data:    assetUser,
	}

	return asset, nil
}

func (e *Extractor) fetchUsers(ctx context.Context) (*admin.Users, error) {
	users, err := e.userService.Do()
	if err != nil {
		return nil, fmt.Errorf("error fetching users: %w", err)
	}

	return users, nil
}

func (e *Extractor) buildMapFromGsuiteSlice(value interface{}) (result []interface{}) {
	if value == nil {
		return
	}

	gsuiteSlice := reflect.ValueOf(value)
	if gsuiteSlice.Kind() != reflect.Slice {
		return
	}

	list, ok := gsuiteSlice.Interface().([]interface{})
	if !ok {
		return
	}

	for _, item := range list {
		result = append(result, e.buildMapFromGsuiteMap(item))
	}

	return
}

func (e *Extractor) buildMapFromGsuiteMap(value interface{}) (result map[string]interface{}) {
	if value == nil {
		return
	}

	gsuiteMap := reflect.ValueOf(value)
	if gsuiteMap.Kind() != reflect.Map {
		return
	}

	result = make(map[string]interface{})
	for _, key := range gsuiteMap.MapKeys() {
		keyString := fmt.Sprintf("%v", key.Interface())
		value := gsuiteMap.MapIndex(key).Interface()

		result[keyString] = value
	}

	return
}

func (e *Extractor) buildMapFromGsuiteMapRawMessage(value interface{}) (result map[string]interface{}) {
	if value == nil {
		return
	}

	gsuiteMap := reflect.ValueOf(value)
	if gsuiteMap.Kind() != reflect.Map {
		return
	}

	result = make(map[string]interface{})
	for _, key := range gsuiteMap.MapKeys() {
		keyString := fmt.Sprintf("%v", key.Interface())
		value := gsuiteMap.MapIndex(key)

		msg, ok := value.Interface().(googleapi.RawMessage)
		if !ok {
			continue
		}

		json, err := msg.MarshalJSON()
		if err != nil {
			continue
		}

		result[keyString] = string(json)
	}

	return
}

// init registers the extractor to catalog
func init() {
	if err := registry.Extractors.Register("gsuite", func() plugins.Extractor {
		return New(plugins.GetLog(), &DefaultUsersServiceFactory{})
	}); err != nil {
		panic(err)
	}
}
