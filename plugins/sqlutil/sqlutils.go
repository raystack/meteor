package sqlutil

import (
	"database/sql"
	"fmt"

	"github.com/goto/salt/log"
)

func FetchDBs(db *sql.DB, logger log.Logger, query string) ([]string, error) {
	res, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("fetch databases: %w", err)
	}
	defer res.Close()

	var dbs []string
	for res.Next() {
		var database string
		if err := res.Scan(&database); err != nil {
			logger.Error("failed to scan, skipping database", "error", err)
			continue
		}
		dbs = append(dbs, database)

	}
	if err := res.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}
	return dbs, nil
}

func FetchTablesInDB(db *sql.DB, dbName, query string) ([]string, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("fetch tables in DB %s: %w", dbName, err)
	}
	defer rows.Close()

	var tbls []string
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tbls = append(tbls, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return tbls, err
}

// buildExcludedDBs builds the list of excluded databases
func BuildBoolMap(strList []string) map[string]bool {
	boolMap := make(map[string]bool)
	for _, db := range strList {
		boolMap[db] = true
	}

	return boolMap
}
