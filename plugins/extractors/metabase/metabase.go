package metabase

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

var sampleConfig = `
host: http://localhost:3000
user_id: meteor_tester
password: meteor_pass_1234
label: my-metabase`

// Config hold the set of configuration for the metabase extractor
type Config struct {
	Host      string `mapstructure:"host" validate:"required"`
	Username  string `mapstructure:"username" validate:"required"`
	Password  string `mapstructure:"password" validate:"required"`
	SessionID string `mapstructure:"session_id"`
}

// Extractor manages the extraction of data
// from the metabase server
type Extractor struct {
	config Config
	logger log.Logger
	client Client
}

// New returns a pointer to an initialized Extractor Object
func New(client Client, logger log.Logger) *Extractor {
	return &Extractor{
		client: client,
		logger: logger,
	}
}

// Info returns the brief information of the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Dashboard list from Metabase server.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	// build and validate config
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	err = e.client.Authenticate(e.config.Host, e.config.Username, e.config.Password, e.config.SessionID)
	if err != nil {
		return errors.Wrap(err, "error initiating client")
	}

	return nil
}

// Extract collects the metadata from the source. The metadata is collected through the out channel
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	dashboards, err := e.client.GetDashboards()
	if err != nil {
		return errors.Wrap(err, "failed to fetch dashboard list")
	}
	for _, d := range dashboards {
		// we do not use "d" as the dashboard because it does not have
		// "ordered_cards" field
		dashboard, err := e.buildDashboard(d)
		if err != nil {
			return errors.Wrap(err, "failed to fetch dashboard data")
		}
		emit(models.NewRecord(dashboard))
	}
	return nil
}

func (e *Extractor) buildDashboard(d Dashboard) (data *assets.Dashboard, err error) {
	// we fetch dashboard again individually to get more fields
	dashboard, err := e.client.GetDashboard(d.ID)
	if err != nil {
		err = errors.Wrapf(err, "error fetching dashboard")
		return
	}
	dashboardUrn := fmt.Sprintf("metabase::%s/dashboard/%d", e.config.Host, dashboard.ID)

	charts, err := e.buildCharts(dashboardUrn, dashboard)
	if err != nil {
		err = errors.Wrapf(err, "error building charts")
		return
	}

	data = &assets.Dashboard{
		Resource: &common.Resource{
			Urn:         dashboardUrn,
			Name:        dashboard.Name,
			Service:     "metabase",
			Description: dashboard.Description,
		},
		Charts: charts,
		Properties: &facets.Properties{
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"id":            dashboard.ID,
				"collection_id": dashboard.CollectionID,
				"creator_id":    dashboard.CreatorID,
			}),
		},
		Timestamps: &common.Timestamp{
			CreateTime: timestamppb.New(time.Time(dashboard.CreatedAt)),
			UpdateTime: timestamppb.New(time.Time(dashboard.UpdatedAt)),
		},
	}
	return
}

func (e *Extractor) buildCharts(dashboardUrn string, dashboard Dashboard) (charts []*assets.Chart, err error) {
	for _, oc := range dashboard.OrderedCards {
		card := oc.Card
		charts = append(charts, &assets.Chart{
			Urn:          fmt.Sprintf("metabase::%s/card/%d", e.config.Host, card.ID),
			DashboardUrn: dashboardUrn,
			Source:       "metabase",
			Name:         card.Name,
			Description:  card.Description,
			Properties: &facets.Properties{
				Attributes: utils.TryParseMapToProto(map[string]interface{}{
					"id":                     card.ID,
					"collection_id":          card.CollectionID,
					"creator_id":             card.CreatorID,
					"database_id":            card.DatabaseID,
					"table_id":               card.TableID,
					"query_average_duration": card.QueryAverageDuration,
					"display":                card.Display,
					"archived":               card.Archived,
				}),
			},
		})
	}

	return
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("metabase", func() plugins.Extractor {
		return New(newClient(), plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
