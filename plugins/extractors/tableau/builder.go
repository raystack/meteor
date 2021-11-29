package tableau

import (
	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/pkg/errors"
)

func (e *Extractor) buildLineage(tables []*Table) (lineage *facets.Lineage) {
	upstreamLineages := []*common.Resource{}
	for _, t := range tables {
		res, err := e.buildLineageResources(t)
		if err != nil {
			e.logger.Warn("failed to build upstreams", "err", err.Error(), "table_id", t.ID, "table_name", t.Name)
			continue
		}
		upstreamLineages = append(upstreamLineages, res)
	}
	lineage = &facets.Lineage{Upstreams: upstreamLineages}
	return
}

func (e *Extractor) buildLineageResources(t *Table) (resource *common.Resource, err error) {
	if t == nil {
		err = errors.New("no table found")
		return
	}
	var table = *t

	upstreamDB := t.Database
	if _, found := upstreamDB["hostName"]; found {
		// DatabaseServer
		var db DatabaseServer
		err = mapstructure.Decode(upstreamDB, &db)
		if err != nil {
			err = errors.Wrap(err, "error cast database to DatabaseServer struct")
			return
		}
		resource = db.CreateResource(table)
		return
	}
	if _, found := upstreamDB["provider"]; found {
		// CloudFile
		var db CloudFile
		err = mapstructure.Decode(upstreamDB, &db)
		if err != nil {
			err = errors.Wrap(err, "error cast database to CloudFile struct")
			return
		}
		resource = db.CreateResource(table)
		return
	}
	if _, found := upstreamDB["filePath"]; found {
		// File
		var db File
		err = mapstructure.Decode(upstreamDB, &db)
		if err != nil {
			err = errors.Wrap(err, "error cast database to File struct")
			return
		}
		resource = db.CreateResource(table)
		return
	}
	if _, found := upstreamDB["connectorUrl"]; found {
		// WebDataConnector
		var db WebDataConnector
		err = mapstructure.Decode(upstreamDB, &db)
		if err != nil {
			err = errors.Wrap(err, "error cast database to WebDataConnector struct")
			return
		}
		resource = db.CreateResource(table)
		return
	}
	err = errors.New("cannot build lineage resource, database structure unknown")
	return
}
