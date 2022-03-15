package utils

import (
	"database/sql"

	"github.com/odpf/meteor/plugins"
)

// Extractor manages the extraction of data from MySQL
type BaseExtractor struct {
	ExcludedDbs map[string]bool
	DB          *sql.DB
	Emit        plugins.Emit
}
