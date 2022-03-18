package utils

import (
	"database/sql"

	"github.com/odpf/salt/log"
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
