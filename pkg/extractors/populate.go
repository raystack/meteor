package extractors

import (
	"github.com/odpf/meteor/extractors"
	"github.com/odpf/meteor/pkg/extractors/bigquerydataset"
	"github.com/odpf/meteor/pkg/extractors/bigquerytable"
	"github.com/odpf/meteor/pkg/extractors/kafka"
	"github.com/odpf/meteor/pkg/extractors/mongodb"
	"github.com/odpf/meteor/pkg/extractors/mssql"
	"github.com/odpf/meteor/pkg/extractors/mysql"
	"github.com/odpf/meteor/pkg/extractors/postgres"
)

func PopulateFactory(factory *extractors.Factory) {
	factory.Set("kafka", func() extractors.Extractor { return new(kafka.Extractor) })
	factory.Set("bigquerydataset", func() extractors.Extractor { return new(bigquerydataset.Extractor) })
	factory.Set("bigquerytable", func() extractors.Extractor { return new(bigquerytable.Extractor) })
	factory.Set("mysql", func() extractors.Extractor { return new(mysql.Extractor) })
	factory.Set("mssql", func() extractors.Extractor { return new(mssql.Extractor) })
	factory.Set("mongodb", func() extractors.Extractor { return new(mongodb.Extractor) })
	factory.Set("postgres", func() extractors.Extractor { return new(postgres.Extractor) })
}
