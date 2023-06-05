package tableau

import (
	"fmt"

	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

func (e *Extractor) buildLineage(tables []*Table) *v1beta2.Lineage {
	var upstreamLineages []*v1beta2.Resource
	for _, t := range tables {
		res, err := e.buildLineageResources(t)
		if err != nil {
			e.logger.Warn("failed to build upstreams", "err", err.Error(), "table_id", t.ID, "table_name", t.Name)
			continue
		}
		upstreamLineages = append(upstreamLineages, res)
	}
	return &v1beta2.Lineage{Upstreams: upstreamLineages}
}

func (*Extractor) buildLineageResources(t *Table) (*v1beta2.Resource, error) {
	if t == nil {
		return nil, errors.New("no table found")
	}

	upstreamDB := t.Database
	if _, found := upstreamDB["hostName"]; found {
		// DatabaseServer
		var db DatabaseServer
		if err := mapstructure.Decode(upstreamDB, &db); err != nil {
			return nil, fmt.Errorf("decode upstream as DatabaseServer: %w", err)
		}

		return db.CreateResource(*t), nil
	}
	if _, found := upstreamDB["provider"]; found {
		// CloudFile
		var db CloudFile
		if err := mapstructure.Decode(upstreamDB, &db); err != nil {
			return nil, fmt.Errorf("decode upstream as CloudFile: %w", err)
		}

		return db.CreateResource(*t), nil
	}
	if _, found := upstreamDB["filePath"]; found {
		// File
		var db File
		if err := mapstructure.Decode(upstreamDB, &db); err != nil {
			return nil, fmt.Errorf("decode upstream as File: %w", err)
		}

		return db.CreateResource(*t), nil
	}
	if _, found := upstreamDB["connectorUrl"]; found {
		// WebDataConnector
		var db WebDataConnector
		if err := mapstructure.Decode(upstreamDB, &db); err != nil {
			return nil, fmt.Errorf("decode upstream as WebDataConnector: %w", err)
		}

		return db.CreateResource(*t), nil
	}
	return nil, errors.New("build lineage resource: database structure unknown")
}
