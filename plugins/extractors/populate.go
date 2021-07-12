package extractors

import (
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins/extractors/bigquerydataset"
	"github.com/odpf/meteor/plugins/extractors/bigquerytable"
	"github.com/odpf/meteor/plugins/extractors/kafka"
	"github.com/odpf/meteor/plugins/extractors/mongodb"
	"github.com/odpf/meteor/plugins/extractors/mssql"
	"github.com/odpf/meteor/plugins/extractors/mysql"
	"github.com/odpf/meteor/plugins/extractors/postgres"
)

func PopulateFactory(factory *extractor.Factory) {
	factory.Set("kafka", func() extractor.Extractor { return new(kafka.Extractor) })
	factory.Set("bigquerydataset", func() extractor.Extractor { return new(bigquerydataset.Extractor) })
	factory.Set("bigquerytable", func() extractor.Extractor { return new(bigquerytable.Extractor) })
	factory.Set("mysql", func() extractor.Extractor { return new(mysql.Extractor) })
	factory.Set("mssql", func() extractor.Extractor { return new(mssql.Extractor) })
	factory.Set("mongodb", func() extractor.Extractor { return new(mongodb.Extractor) })
	factory.Set("postgres", func() extractor.Extractor { return new(postgres.Extractor) })
}
