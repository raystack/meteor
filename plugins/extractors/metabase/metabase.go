package metabase

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"strings"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/utils"
	"github.com/goto/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/anypb"
)

//go:embed README.md
var summary string

var sampleConfig = `
host: http://localhost:3000
instance_label: my-metabase
username: meteor_tester
password: meteor_pass_1234`

var info = plugins.Info{
	Description:  "Dashboard list from Metabase server.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Config holds the set of configuration for the metabase extractor
type Config struct {
	Host          string `mapstructure:"host" validate:"required"`
	InstanceLabel string `mapstructure:"instance_label" validate:"required"`
	Username      string `mapstructure:"username" validate:"required_without=SessionID"`
	Password      string `mapstructure:"password"`
	SessionID     string `mapstructure:"session_id"`
}

// Extractor manages the extraction of data
// from the metabase server
type Extractor struct {
	plugins.BaseExtractor
	config Config
	logger log.Logger
	client Client
}

// New returns a pointer to an initialized Extractor Object
func New(client Client, logger log.Logger) *Extractor {
	e := &Extractor{
		client: client,
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
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
		dashboard, err := e.buildDashboard(d)
		if err != nil {
			e.logger.Error("failed to build dashboard with", "dashboard_id", d.ID, "err", err.Error())
			continue
		}

		emit(models.NewRecord(dashboard))
	}
	return nil
}

func (e *Extractor) buildDashboard(d Dashboard) (asset *v1beta2.Asset, err error) {
	// we fetch dashboard again individually to get more fields
	dashboard, err := e.client.GetDashboard(d.ID)
	if err != nil {
		err = errors.Wrapf(err, "error fetching dashboard")
		return
	}

	dashboardUrn := models.NewURN("metabase", e.UrnScope, "collection", fmt.Sprintf("%d", dashboard.ID))
	charts := e.buildCharts(dashboardUrn, dashboard)
	dashboardUpstreams := e.buildDashboardUpstreams(charts)

	data, err := anypb.New(&v1beta2.Dashboard{
		Charts: charts,
		Attributes: utils.TryParseMapToProto(map[string]interface{}{
			"id":            dashboard.ID,
			"collection_id": dashboard.CollectionID,
			"creator_id":    dashboard.CreatorID,
		}),
		CreateTime: dashboard.CreatedAt.ToPB(),
		UpdateTime: dashboard.UpdatedAt.ToPB(),
	})
	if err != nil {
		err = fmt.Errorf("error creating Any struct: %w", err)
	}

	asset = &v1beta2.Asset{
		Urn:         dashboardUrn,
		Name:        dashboard.Name,
		Service:     "metabase",
		Type:        "dashboard",
		Description: dashboard.Description,
		Data:        data,
		Lineage: &v1beta2.Lineage{
			Upstreams: dashboardUpstreams,
		},
	}
	return
}

func (e *Extractor) buildCharts(dashboardUrn string, dashboard Dashboard) (charts []*v1beta2.Chart) {
	for _, oc := range dashboard.OrderedCards {
		chart, err := e.buildChart(oc.Card, dashboardUrn)
		if err != nil {
			e.logger.Error("error building upstreams for a card", "card_id", oc.Card.ID, "err", err)
		} else {
			charts = append(charts, chart)
		}
	}

	return
}

func (e *Extractor) buildChart(card Card, dashboardUrn string) (chart *v1beta2.Chart, err error) {
	var upstreams []*v1beta2.Resource
	upstreams, err = e.buildUpstreams(card)
	if err != nil {
		e.logger.Warn("error building upstreams for a card", "card_id", card.ID, "err", err)
	}

	return &v1beta2.Chart{
		Urn:          fmt.Sprintf("metabase::%s/card/%d", e.config.InstanceLabel, card.ID),
		DashboardUrn: dashboardUrn,
		Source:       "metabase",
		Name:         card.Name,
		Description:  card.Description,
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
		Lineage: &v1beta2.Lineage{
			Upstreams: upstreams,
		},
	}, nil
}

func (e *Extractor) buildUpstreams(card Card) (upstreams []*v1beta2.Resource, err error) {
	switch card.DatasetQuery.Type {
	case datasetQueryTypeQuery:
		upstreams, err = e.buildUpstreamsFromQuery(card)
		if err != nil {
			err = errors.Wrap(err, "error building upstreams from query")
		}
		return
	case datasetQueryTypeNative:
		upstreams, err = e.buildUpstreamsFromNative(card)
		if err != nil {
			err = errors.Wrap(err, "error building upstreams from native")
		}
		return
	default:
		return
	}
}

func (e *Extractor) buildUpstreamsFromQuery(card Card) (upstreams []*v1beta2.Resource, err error) {
	table, err := e.client.GetTable(card.DatasetQuery.Query.SourceTable)
	if err != nil {
		err = errors.Wrap(err, "error getting table")
		return
	}

	service, cluster, dbName := e.extractDbComponent(table.Db)
	upstreams = append(upstreams, &v1beta2.Resource{
		Urn:     e.buildURN(service, cluster, dbName, table.Name),
		Service: service,
		Type:    "table",
	})

	return
}

func (e *Extractor) buildUpstreamsFromNative(card Card) (upstreams []*v1beta2.Resource, err error) {
	database, err := e.client.GetDatabase(card.DatasetQuery.Database)
	if err != nil {
		err = errors.Wrap(err, "error getting database")
		return
	}

	tableNames, err := e.getTableNamesFromSQL(card.DatasetQuery.Native)
	if err != nil {
		err = errors.Wrap(err, "error getting table names from SQL")
		return
	}

	service, cluster, dbName := e.extractDbComponent(database)
	for _, tableName := range tableNames {
		upstreams = append(upstreams, &v1beta2.Resource{
			Urn:     e.buildURN(service, cluster, dbName, tableName),
			Service: service,
			Type:    "table",
		})
	}

	return
}

func (e *Extractor) buildDashboardUpstreams(charts []*v1beta2.Chart) (upstreams []*v1beta2.Resource) {
	existing := map[string]bool{}
	for _, chart := range charts {
		if chart.Lineage == nil {
			continue
		}

		for _, upstream := range chart.Lineage.Upstreams {
			if _, duplicate := existing[upstream.Urn]; duplicate {
				continue
			}

			upstreams = append(upstreams, upstream)
			existing[upstream.Urn] = true
		}
	}

	return
}

func (e *Extractor) extractDbComponent(database Database) (service, cluster, dbName string) {
	service = database.Engine

	switch service {
	case "h2":
		comps := strings.Split(database.Details.Db, ";")
		dbUrlComps := strings.Split(comps[0], "/")

		cluster = strings.Join(dbUrlComps[:len(dbUrlComps)-1], "/")
		dbName = dbUrlComps[len(dbUrlComps)-1]
	case "postgres":
		fallthrough
	case "mysql":
		cluster = fmt.Sprintf("%s:%d", database.Details.Host, database.Details.Port)
		dbName = database.Details.Dbname
	case "bigquery":
		cluster = database.Details.ProjectID
		dbName = database.Details.DatasetID
	default:
		e.logger.Warn(fmt.Sprintf("unsupported database engine \"%s\"", service))
	}

	return
}

func (e *Extractor) getTableNamesFromSQL(datasetQuery NativeDatasetQuery) (tableNames []string, err error) {
	query, err := evaluateQueryTemplate(datasetQuery)
	if err != nil {
		err = errors.Wrap(err, "error adding default value to template in query")
		return
	}
	tableNames, err = extractTableNamesFromSQL(query)
	if err != nil {
		err = errors.Wrap(err, "error when parsing SQL")
	}

	return
}

func (e *Extractor) buildURN(service, cluster, dbName, tableName string) string {
	tableComps := strings.Split(tableName, ".")
	compLength := len(tableComps)
	tableName = tableComps[len(tableComps)-1]

	switch service {
	case "postgres":
		if compLength > 1 && tableComps[0] != "public" {
			cluster = tableComps[0]
		}
	case "bigquery":
		project := cluster
		dataset := dbName
		if compLength > 2 {
			project = tableComps[0]
			dataset = tableComps[1]
		} else if compLength > 1 {
			dataset = tableComps[0]
		}

		return plugins.BigQueryURN(project, dataset, tableName)
	}

	return models.NewURN(service, cluster, "table", fmt.Sprintf("%s.%s", dbName, tableName))
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("metabase", func() plugins.Extractor {
		return New(newClient(), plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
