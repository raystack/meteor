package sqlutil

import (
	"database/sql"

	"github.com/goto/salt/log"
	"github.com/pkg/errors"
)

func FetchDBs(db *sql.DB, logger log.Logger, query string) ([]string, error) {
	res, err := db.Query(query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch databases")
	}

	var dbs []string
	for res.Next() {
		var database string
		err := res.Scan(&database)
		if err != nil {
			return nil, err
		}

		dbs = append(dbs, database)
		if err := res.Scan(&database); err != nil {
			logger.Error("failed to connect, skipping database", "error", err)
			continue
		}

	}
	return dbs, nil
}

func FetchTablesInDB(db *sql.DB, dbName, query string) (list []string, err error) {
	rows, err := db.Query(query)
	if err != nil {
		return
	}
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return
		}
		list = append(list, table)
	}
	return list, err
}

// buildExcludedDBs builds the list of excluded databases
func BuildBoolMap(strList []string) map[string]bool {
	boolMap := make(map[string]bool)
	for _, db := range strList {
		boolMap[db] = true
	}

	return boolMap
}
