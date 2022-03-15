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

// BuildExcludedDBs builds the list of excluded databases
func BuildExcludedDBs(defaultDBList []string) map[string]bool {
	excludedMap := make(map[string]bool)
	for _, db := range defaultDBList {
		excludedMap[db] = true
	}

	return excludedMap
}

// IsExcludedDB checks if the given db is in the list of excluded databases
func IsExcludedDB(database string, excludedDbs map[string]bool) bool {
	_, ok := excludedDbs[database]
	return ok
}

// IsNullable checks if the given string is null or not
func IsNullable(value string) bool {
	return value == "YES"
}
