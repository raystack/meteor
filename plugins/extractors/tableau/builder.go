package tableau

import (
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

func (e *Extractor) buildLineageURNs(tables []*Table) []string {
	var urns []string
	for _, t := range tables {
		urn, err := e.buildLineageResourceURN(t)
		if err != nil {
			e.logger.Warn("failed to build upstreams", "err", err.Error(), "table_id", t.ID, "table_name", t.Name)
			continue
		}
		urns = append(urns, urn)
	}
	return urns
}

func (e *Extractor) buildLineageResourceURN(t *Table) (string, error) {
	if t == nil {
		return "", errors.New("no table found")
	}
	var table = *t

	upstreamDB := t.Database
	if _, found := upstreamDB["hostName"]; found {
		// DatabaseServer
		var db DatabaseServer
		err := mapstructure.Decode(upstreamDB, &db)
		if err != nil {
			return "", errors.Wrap(err, "error cast database to DatabaseServer struct")
		}
		return db.CreateResourceURN(table), nil
	}
	if _, found := upstreamDB["provider"]; found {
		// CloudFile
		var db CloudFile
		err := mapstructure.Decode(upstreamDB, &db)
		if err != nil {
			return "", errors.Wrap(err, "error cast database to CloudFile struct")
		}
		return db.CreateResourceURN(table), nil
	}
	if _, found := upstreamDB["filePath"]; found {
		// File
		var db File
		err := mapstructure.Decode(upstreamDB, &db)
		if err != nil {
			return "", errors.Wrap(err, "error cast database to File struct")
		}
		return db.CreateResourceURN(table), nil
	}
	if _, found := upstreamDB["connectorUrl"]; found {
		// WebDataConnector
		var db WebDataConnector
		err := mapstructure.Decode(upstreamDB, &db)
		if err != nil {
			return "", errors.Wrap(err, "error cast database to WebDataConnector struct")
		}
		return db.CreateResourceURN(table), nil
	}
	return "", errors.New("cannot build lineage resource, database structure unknown")
}
