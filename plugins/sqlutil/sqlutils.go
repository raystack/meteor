package sqlutil

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/goto/salt/log"
	"go.nhat.io/otelsql"
	"go.opentelemetry.io/otel/attribute"
)

func FetchDBs(ctx context.Context, db *sql.DB, logger log.Logger, query string) ([]string, error) {
	res, err := db.QueryContext(ctx, query)
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

func FetchTablesInDB(ctx context.Context, db *sql.DB, dbName, query string) ([]string, error) {
	rows, err := db.QueryContext(ctx, query)
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

func OpenWithOtel(driverName, connectionURL string, otelSemConv attribute.KeyValue) (db *sql.DB, err error) {
	driverName, err = otelsql.Register(driverName,
		otelsql.TraceQueryWithoutArgs(),
		otelsql.TraceRowsClose(),
		otelsql.TraceRowsAffected(),
		otelsql.WithSystem(otelSemConv),
	)
	if err != nil {
		return nil, fmt.Errorf("register %s otelsql wrapper: %w", driverName, err)
	}

	db, err = sql.Open(driverName, connectionURL)
	if err != nil {
		return nil, err
	}

	if err := otelsql.RecordStats(db); err != nil {
		return nil, err
	}

	return db, nil
}
