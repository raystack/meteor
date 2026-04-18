package metabase

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"strings"
	"time"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/metabase/client"
	m "github.com/raystack/meteor/plugins/extractors/metabase/models"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
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
	Host          string `json:"host" yaml:"host" mapstructure:"host" validate:"required"`
	InstanceLabel string `json:"instance_label" yaml:"instance_label" mapstructure:"instance_label" validate:"required"`
	Username      string `json:"username" yaml:"username" mapstructure:"username" validate:"required_without=SessionID"`
	Password      string `json:"password" yaml:"password" mapstructure:"password"`
	SessionID     string `json:"session_id" yaml:"session_id" mapstructure:"session_id"`
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
		record, err := e.buildDashboard(ctx, d)
		if err != nil {
			e.logger.Error("failed to build dashboard with", "dashboard_id", d.ID, "err", err.Error())
			continue
		}

		emit(record)
	}
	return nil
}

// chartUpstream holds a chart map and its upstream URNs
type chartUpstream struct {
	chart     map[string]any
	upstreams []string
}

func (e *Extractor) buildDashboard(ctx context.Context, d m.Dashboard) (models.Record, error) {
	// we fetch dashboard again individually to get more fields
	dashboard, err := e.client.GetDashboard(ctx, d.ID)
	if err != nil {
		return models.Record{}, fmt.Errorf("fetch database: %w", err)
	}

	dashboardURN := models.NewURN("metabase", e.UrnScope, "collection", fmt.Sprintf("%d", dashboard.ID))
	chartData := e.buildCharts(ctx, dashboardURN, dashboard)

	// Collect unique upstream URNs from all charts for dashboard-level lineage
	var edges []*meteorv1beta1.Edge
	existing := map[string]bool{}
	for _, cu := range chartData {
		for _, upstreamURN := range cu.upstreams {
			if !existing[upstreamURN] {
				edges = append(edges, models.LineageEdge(upstreamURN, dashboardURN, "metabase"))
				existing[upstreamURN] = true
			}
		}
	}

	// Create owned_by edge to the dashboard creator.
	if dashboard.CreatorID > 0 {
		ownerURN := models.NewURN("metabase", e.UrnScope, "user", fmt.Sprintf("%d", dashboard.CreatorID))
		edges = append(edges, models.OwnerEdge(dashboardURN, ownerURN, "metabase"))
	}

	charts := make([]map[string]any, 0, len(chartData))
	for _, cu := range chartData {
		charts = append(charts, cu.chart)
	}

	props := map[string]any{
		"charts":        charts,
		"id":            dashboard.ID,
		"collection_id": dashboard.CollectionID,
		"creator_id":    dashboard.CreatorID,
	}
	if !time.Time(dashboard.CreatedAt).IsZero() {
		props["create_time"] = time.Time(dashboard.CreatedAt).Format("2006-01-02T15:04:05Z")
	}
	if !time.Time(dashboard.UpdatedAt).IsZero() {
		props["update_time"] = time.Time(dashboard.UpdatedAt).Format("2006-01-02T15:04:05Z")
	}

	entity := models.NewEntity(dashboardURN, "dashboard", dashboard.Name, "metabase", props)
	if dashboard.Description != "" {
		entity.Description = dashboard.Description
	}
	return models.NewRecord(entity, edges...), nil
}

func (e *Extractor) buildCharts(ctx context.Context, dashboardURN string, dashboard m.Dashboard) []chartUpstream {
	var charts []chartUpstream
	for _, oc := range dashboard.OrderedCards {
		cu, err := e.buildChart(ctx, oc.Card, dashboardURN)
		if err != nil {
			e.logger.Error(
				"error building chart for dashboard",
				"dashboard_urn", dashboardURN,
				"card_id", oc.Card.ID,
				"err", err,
			)
			continue
		}
		charts = append(charts, cu)
	}

	return charts
}

func (e *Extractor) buildChart(ctx context.Context, card m.Card, dashboardURN string) (chartUpstream, error) {
	upstreamURNs, err := e.buildUpstreamURNs(ctx, card)
	if err != nil {
		e.logger.Warn("error building upstreams for a card", "card_id", card.ID, "err", err)
	}

	chart := map[string]any{
		"urn":                    fmt.Sprintf("metabase::%s/card/%d", e.config.InstanceLabel, card.ID),
		"dashboard_urn":          dashboardURN,
		"source":                 "metabase",
		"name":                   card.Name,
		"id":                     card.ID,
		"collection_id":          card.CollectionID,
		"creator_id":             card.CreatorID,
		"database_id":            card.DatabaseID,
		"table_id":               card.TableID,
		"query_average_duration": card.QueryAverageDuration,
		"display":                card.Display,
		"archived":               card.Archived,
	}
	if card.Description != "" {
		chart["description"] = card.Description
	}
	if len(upstreamURNs) > 0 {
		chart["upstreams"] = upstreamURNs
	}

	return chartUpstream{chart: chart, upstreams: upstreamURNs}, nil
}

func (e *Extractor) buildUpstreamURNs(ctx context.Context, card m.Card) ([]string, error) {
	switch card.DatasetQuery.Type {
	case datasetQueryTypeQuery:
		return e.buildUpstreamURNsFromQuery(ctx, card)
	case datasetQueryTypeNative:
		return e.buildUpstreamURNsFromNative(ctx, card)
	default:
		return nil, nil
	}
}

func (e *Extractor) buildUpstreamURNsFromQuery(ctx context.Context, card m.Card) ([]string, error) {
	table, err := e.client.GetTable(ctx, card.DatasetQuery.Query.SourceTable)
	if err != nil {
		return nil, fmt.Errorf("get table: %w", err)
	}

	service, cluster, dbName := e.extractDBComponent(table.Db)
	return []string{e.buildURN(service, cluster, dbName, table.Name)}, nil
}

func (e *Extractor) buildUpstreamURNsFromNative(ctx context.Context, card m.Card) ([]string, error) {
	database, err := e.client.GetDatabase(ctx, card.DatasetQuery.Database)
	if err != nil {
		return nil, fmt.Errorf("get database: %w", err)
	}

	tableNames, err := e.getTableNamesFromSQL(card.DatasetQuery.Native)
	if err != nil {
		return nil, fmt.Errorf("extract table names from SQL: %w", err)
	}

	var urns []string
	service, cluster, dbName := e.extractDBComponent(database)
	for _, tableName := range tableNames {
		urns = append(urns, e.buildURN(service, cluster, dbName, tableName))
	}

	return urns, nil
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
