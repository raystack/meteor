package metabase

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"strings"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/metabase/client"
	m "github.com/goto/meteor/plugins/extractors/metabase/models"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/utils"
	"github.com/goto/salt/log"
	"google.golang.org/protobuf/types/known/anypb"
)

//go:embed README.md
var summary string

const (
	datasetQueryTypeQuery  = "query"
	datasetQueryTypeNative = "native"
)

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
	client client.Client
}

// New returns a pointer to an initialized Extractor Object
func New(c client.Client, l log.Logger) *Extractor {
	e := &Extractor{
		client: c,
		logger: l,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	err := e.client.Authenticate(ctx, e.config.Host, e.config.Username, e.config.Password, e.config.SessionID)
	if err != nil {
		return fmt.Errorf("initiate client: %w", err)
	}

	return nil
}

// Extract collects the metadata from the source. The metadata is collected through the out channel
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	dashboards, err := e.client.GetDashboards(ctx)
	if err != nil {
		return fmt.Errorf("fetch dashboard list: %w", err)
	}
	for _, d := range dashboards {
		dashboard, err := e.buildDashboard(ctx, d)
		if err != nil {
			e.logger.Error("failed to build dashboard with", "dashboard_id", d.ID, "err", err.Error())
			continue
		}

		emit(models.NewRecord(dashboard))
	}
	return nil
}

func (e *Extractor) buildDashboard(ctx context.Context, d m.Dashboard) (*v1beta2.Asset, error) {
	// we fetch dashboard again individually to get more fields
	dashboard, err := e.client.GetDashboard(ctx, d.ID)
	if err != nil {
		return nil, fmt.Errorf("fetch database: %w", err)
	}

	dashboardURN := models.NewURN("metabase", e.UrnScope, "collection", fmt.Sprintf("%d", dashboard.ID))
	charts := e.buildCharts(ctx, dashboardURN, dashboard)
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
		return nil, fmt.Errorf("create Any struct: %w", err)
	}

	return &v1beta2.Asset{
		Urn:         dashboardURN,
		Name:        dashboard.Name,
		Service:     "metabase",
		Type:        "dashboard",
		Description: dashboard.Description,
		Data:        data,
		Lineage: &v1beta2.Lineage{
			Upstreams: dashboardUpstreams,
		},
	}, nil
}

func (e *Extractor) buildCharts(ctx context.Context, dashboardURN string, dashboard m.Dashboard) []*v1beta2.Chart {
	var charts []*v1beta2.Chart
	for _, oc := range dashboard.OrderedCards {
		chart, err := e.buildChart(ctx, oc.Card, dashboardURN)
		if err != nil {
			e.logger.Error(
				"error building chart for dashboard",
				"dashboard_urn", dashboardURN,
				"card_id", oc.Card.ID,
				"err", err,
			)
			continue
		}
		charts = append(charts, chart)

	}

	return charts
}

func (e *Extractor) buildChart(ctx context.Context, card m.Card, dashboardURN string) (*v1beta2.Chart, error) {
	upstreams, err := e.buildUpstreams(ctx, card)
	if err != nil {
		e.logger.Warn("error building upstreams for a card", "card_id", card.ID, "err", err)
	}

	return &v1beta2.Chart{
		Urn:          fmt.Sprintf("metabase::%s/card/%d", e.config.InstanceLabel, card.ID),
		DashboardUrn: dashboardURN,
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

func (e *Extractor) buildUpstreams(ctx context.Context, card m.Card) ([]*v1beta2.Resource, error) {
	switch card.DatasetQuery.Type {
	case datasetQueryTypeQuery:
		upstreams, err := e.buildUpstreamsFromQuery(ctx, card)
		if err != nil {
			return nil, fmt.Errorf("build upstreams from query: %w", err)
		}
		return upstreams, nil

	case datasetQueryTypeNative:
		upstreams, err := e.buildUpstreamsFromNative(ctx, card)
		if err != nil {
			return nil, fmt.Errorf("build upstreams from native: %w", err)
		}
		return upstreams, nil

	default:
		return nil, nil
	}
}

func (e *Extractor) buildUpstreamsFromQuery(ctx context.Context, card m.Card) ([]*v1beta2.Resource, error) {
	table, err := e.client.GetTable(ctx, card.DatasetQuery.Query.SourceTable)
	if err != nil {
		return nil, fmt.Errorf("get table: %w", err)
	}

	service, cluster, dbName := e.extractDBComponent(table.Db)
	return []*v1beta2.Resource{{
		Urn:     e.buildURN(service, cluster, dbName, table.Name),
		Service: service,
		Type:    "table",
	}}, nil
}

func (e *Extractor) buildUpstreamsFromNative(ctx context.Context, card m.Card) ([]*v1beta2.Resource, error) {
	database, err := e.client.GetDatabase(ctx, card.DatasetQuery.Database)
	if err != nil {
		return nil, fmt.Errorf("get database: %w", err)
	}

	tableNames, err := e.getTableNamesFromSQL(card.DatasetQuery.Native)
	if err != nil {
		return nil, fmt.Errorf("extract table names from SQL: %w", err)
	}

	var upstreams []*v1beta2.Resource
	service, cluster, dbName := e.extractDBComponent(database)
	for _, tableName := range tableNames {
		upstreams = append(upstreams, &v1beta2.Resource{
			Urn:     e.buildURN(service, cluster, dbName, tableName),
			Service: service,
			Type:    "table",
		})
	}

	return upstreams, nil
}

func (*Extractor) buildDashboardUpstreams(charts []*v1beta2.Chart) []*v1beta2.Resource {
	var upstreams []*v1beta2.Resource
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

	return upstreams
}

func (e *Extractor) extractDBComponent(database m.Database) (service, cluster, dbName string) {
	service = database.Engine

	switch service {
	case "h2":
		comps := strings.Split(database.Details.Db, ";")
		dbUrlComps := strings.Split(comps[0], "/")

		cluster = strings.Join(dbUrlComps[:len(dbUrlComps)-1], "/")
		dbName = dbUrlComps[len(dbUrlComps)-1]
	case "postgres", "mysql":
		cluster = fmt.Sprintf("%s:%d", database.Details.Host, database.Details.Port)
		dbName = database.Details.Dbname
	case "bigquery":
		cluster = database.Details.ProjectID
		dbName = database.Details.DatasetID
	default:
		e.logger.Warn("unsupported database engine", "database_engine", service)
	}

	return service, cluster, dbName
}

func (*Extractor) getTableNamesFromSQL(datasetQuery m.NativeDatasetQuery) ([]string, error) {
	query, err := evaluateQueryTemplate(datasetQuery)
	if err != nil {
		return nil, fmt.Errorf("error evaluating query template: %w", err)
	}

	tableNames, err := extractTableNamesFromSQL(query)
	if err != nil {
		return nil, fmt.Errorf("parse SQL: %w", err)
	}

	return tableNames, nil
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
		return New(client.New(), plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
