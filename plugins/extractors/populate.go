package extractors

import (
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins/extractors/bigquery"
	"github.com/odpf/meteor/plugins/extractors/kafka"
	"github.com/odpf/meteor/plugins/extractors/mongodb"
	"github.com/odpf/meteor/plugins/extractors/mssql"
	"github.com/odpf/meteor/plugins/extractors/mysql"
	"github.com/odpf/meteor/plugins/extractors/postgres"
)

func PopulateFactory(factory *extractor.Factory) {
	// populate topic extractors
	factory.SetTopicExtractor("kafka", kafka.New)

	// populate table extractors
	factory.SetTableExtractor("bigquery", bigquery.New)
	factory.SetTableExtractor("mysql", mysql.New)
	factory.SetTableExtractor("mssql", mssql.New)
	factory.SetTableExtractor("mongodb", mongodb.New)
	factory.SetTableExtractor("postgres", postgres.New)
}
