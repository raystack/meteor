package googleworkspace

import (
	"context"
	_ "embed" // used to print the embedded assets

	"github.com/pkg/errors"

	"github.com/odpf/meteor/models"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"golang.org/x/oauth2/google"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/option"
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
	r, err := e.client.Users.List().Customer("my_customer").MaxResults(10).
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

			emit(models.NewRecord(&assetsv1beta1.User{
				Email:    u.PrimaryEmail,
				FullName: u.Name.FullName,
				LastName: u.Name.FamilyName,
				Status:   status,
				Properties: &facetsv1beta1.Properties{
					Attributes: utils.TryParseMapToProto(map[string]interface{}{
						"organisation":   u.Organizations,
						"relation":       u.Relations,
						"custom_schemas": u.CustomSchemas,
						"org_unit_path":  u.OrgUnitPath,
					}),
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
