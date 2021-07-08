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
	store.Populate(map[string]extractors.Extractor{
		"kafka":           new(kafka.Extractor),
		"bigquerydataset": new(bigquerydataset.Extractor),
		"bigquerytable":   new(bigquerytable.Extractor),
		"mysql":           new(mysql.Extractor),
		"mssql":           new(mssql.Extractor),
		"mongodb":         new(mongodb.Extractor),
		"postgres":        new(postgres.Extractor),
	})
}
