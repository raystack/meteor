package gsuite

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"reflect"
	"strings"

	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
)

//go:embed README.md
var summary string

type Config struct {
	ServiceAccountJSON string `json:"service_account_json" yaml:"service_account_json" mapstructure:"service_account_json" validate:"required"`
	UserEmail          string `json:"user_email" yaml:"user_email" mapstructure:"user_email" validate:"required"`
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
user_email: user@raystack.com`

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
// The data is returned as a list of Records.
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
		record := e.buildRecord(u)
		e.emit(record)
	}

	return nil
}

func (e *Extractor) buildRecord(gsuiteUser *admin.User) models.Record {
	var status string
	if gsuiteUser.Suspended {
		status = "suspended"
	}

	props := map[string]any{
		"email":     gsuiteUser.PrimaryEmail,
		"full_name": gsuiteUser.Name.FullName,
	}
	if status != "" {
		props["status"] = status
	}

	organizations := e.buildMapFromGsuiteSlice(gsuiteUser.Organizations)
	if len(organizations) > 0 {
		props["organizations"] = organizations
	}
	relations := e.buildMapFromGsuiteSlice(gsuiteUser.Relations)
	if len(relations) > 0 {
		props["relations"] = relations
	}
	customSchemas := e.buildMapFromGsuiteMapRawMessage(gsuiteUser.CustomSchemas)
	if len(customSchemas) > 0 {
		props["custom_schemas"] = customSchemas
	}
	if len(gsuiteUser.Aliases) > 0 {
		props["aliases"] = strings.Join(gsuiteUser.Aliases, ",")
	}
	if gsuiteUser.OrgUnitPath != "" {
		props["org_unit_path"] = gsuiteUser.OrgUnitPath
	}

	entity := models.NewEntity(
		models.NewURN("gsuite", e.UrnScope, "user", gsuiteUser.PrimaryEmail),
		"user",
		gsuiteUser.Name.FullName,
		"gsuite",
		props,
	)

	return models.NewRecord(entity)
}

func (e *Extractor) fetchUsers(ctx context.Context) (*admin.Users, error) {
	users, err := e.userService.Do()
	if err != nil {
		return nil, fmt.Errorf("error fetching users: %w", err)
	}

	return users, nil
}

func (e *Extractor) buildMapFromGsuiteSlice(value any) (result []any) {
	if value == nil {
		return
	}

	gsuiteSlice := reflect.ValueOf(value)
	if gsuiteSlice.Kind() != reflect.Slice {
		return
	}

	list, ok := gsuiteSlice.Interface().([]any)
	if !ok {
		return
	}

	for _, item := range list {
		result = append(result, e.buildMapFromGsuiteMap(item))
	}

	return
}

func (e *Extractor) buildMapFromGsuiteMap(value any) (result map[string]any) {
	if value == nil {
		return
	}

	gsuiteMap := reflect.ValueOf(value)
	if gsuiteMap.Kind() != reflect.Map {
		return
	}

	result = make(map[string]any)
	for _, key := range gsuiteMap.MapKeys() {
		keyString := fmt.Sprintf("%v", key.Interface())
		value := gsuiteMap.MapIndex(key).Interface()

		result[keyString] = value
	}

	return
}

func (e *Extractor) buildMapFromGsuiteMapRawMessage(value any) (result map[string]any) {
	if value == nil {
		return
	}

	gsuiteMap := reflect.ValueOf(value)
	if gsuiteMap.Kind() != reflect.Map {
		return
	}

	result = make(map[string]any)
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
