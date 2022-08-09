package googleworkspace

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"reflect"

	"github.com/pkg/errors"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"golang.org/x/oauth2/google"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/option"

	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
)

type Config struct {
	ServiceAccountJSON string `mapstructure:"service_account_json" validate:"required"`
	UserEmail          string `mapstructure:"user_email" validate:"required"`
}

var sampleConfig = `
service_account_json: {
    "type": "service_account",
    "project_id": "odpf-project",
    "private_key_id": "3cb2saasa3ef788dvdvdvdvdvdssdvds57",
    "private_key": "-----BEGIN PRIVATE KEY-----\njbjabdjbajd\n-----END PRIVATE KEY-----\n",
    "client_email": "meteor-sa@odpf-project.iam.gserviceaccount.com",
    "client_id": "1100599572858548635286",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/meteor-sa%40odpf-project.iam.gserviceaccount.com"
}
user_email: user@odpf.com`

var info = plugins.Info{
	Description:  "User list from Google Workspace",
	SampleConfig: sampleConfig,
	Tags:         []string{"platform", "extractor"},
}

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	plugins.BaseExtractor
	logger log.Logger
	config Config
	client *admin.Service
	emit   plugins.Emit
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	jwtConfig, err := google.JWTConfigFromJSON([]byte(e.config.ServiceAccountJSON), admin.AdminDirectoryUserScope)
	if err != nil {
		return errors.Wrap(err, "JWTConfigFromJSON")
	}
	jwtConfig.Subject = e.config.UserEmail

	ts := jwtConfig.TokenSource(ctx)

	e.client, err = admin.NewService(ctx, option.WithTokenSource(ts))
	if err != nil {
		return errors.Wrap(err, "NewService")
	}

	return
}

// Extract extracts the data from the extractor
// The data is returned as a list of assets.Asset
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	var status string
	e.emit = emit
	r, err := e.client.Users.List().Customer("my_customer").
		OrderBy("email").Do()
	if err != nil {
		e.logger.Error("Unable to retrieve users in domain: %v", err)
	}

	if len(r.Users) == 0 {
		e.logger.Info("No users found.\n")
	} else {
		for _, u := range r.Users {
			if !u.Suspended {
				status = "not suspended"
			} else {
				status = "suspended"
			}

			var userAttributes = make(map[string]interface{})
			userAttributes = getOrgAttributes(userAttributes, u.Organizations)
			userAttributes = getRelationsAttributes(userAttributes, u.Relations)
			userAttributes = getCustomSchemasAttributes(userAttributes, u.CustomSchemas)
			userAttributes["org_unit_path"] = u.OrgUnitPath
			e.emit(models.NewRecord(&assetsv1beta1.User{
				Resource: &commonv1beta1.Resource{
					Service: "google workspace",
					Name:    u.PrimaryEmail,
				},
				Email:    u.PrimaryEmail,
				FullName: u.Name.FullName,
				LastName: u.Name.FamilyName,
				Status:   status,
				Properties: &facetsv1beta1.Properties{
					Attributes: utils.TryParseMapToProto(userAttributes),
				},
			}))
		}
	}

	return nil
}

// init registers the extractor to catalog
func init() {
	if err := registry.Extractors.Register("googleworkspace", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}

func getOrgAttributes(userAttributes map[string]interface{}, i interface{}) map[string]interface{} {
	if i != nil {
		itr := reflect.ValueOf(i)
		if itr.Kind() == reflect.Slice {
			for idx := 0; idx < itr.Len(); idx++ {
				valMap := reflect.ValueOf(itr.Index(idx).Interface())
				for _, key := range valMap.MapKeys() {
					strct := valMap.MapIndex(key)
					userAttributes[fmt.Sprintf("%v", key.Interface())] = strct.Interface()
				}
			}
		}
	}
	return userAttributes
}

func getRelationsAttributes(userAttributes map[string]interface{}, i interface{}) map[string]interface{} {
	if i != nil {
		itr := reflect.ValueOf(i)
		if itr.Kind() == reflect.Slice {
			for idx := 0; idx < itr.Len(); idx++ {
				valMap := reflect.ValueOf(itr.Index(idx).Interface())
				var relationType, relationValue string
				for _, key := range valMap.MapKeys() {
					strct := valMap.MapIndex(key)
					if key.Interface().(string) == "type" {
						relationType = strct.Interface().(string)
					} else if key.Interface().(string) == "value" {
						relationValue = strct.Interface().(string)
					}
				}
				userAttributes[relationType] = relationValue
			}
		}
	}
	return userAttributes
}

func getCustomSchemasAttributes(userAttributes map[string]interface{}, i interface{}) map[string]interface{} {
	if i != nil {
		itr := reflect.ValueOf(i)
		if itr.Kind() == reflect.Map {
			for _, key := range itr.MapKeys() {
				strct := itr.MapIndex(key)
				userAttributes[fmt.Sprintf("%v", key.Interface())] = strct.Interface()
			}
		}
	}
	return userAttributes
}
