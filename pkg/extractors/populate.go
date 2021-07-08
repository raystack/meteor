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

func PopulateStore(store *extractors.Store) {
	store.Set("kafka", new(kafka.Extractor))
	store.Set("bigquerydataset", new(bigquerydataset.Extractor))
	store.Set("bigquerytable", new(bigquerytable.Extractor))
	store.Set("mysql", new(mysql.Extractor))
	store.Set("mssql", new(mssql.Extractor))
	store.Set("mongodb", new(mongodb.Extractor))
	store.Set("postgres", new(postgres.Extractor))
}
