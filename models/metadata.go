package models

import (
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
)

// Metadata is a wrapper for the meta
type Metadata interface {
	GetResource() *common.Resource
	GetProperties() *facets.Properties
}

type LineageMetadata interface {
	GetLineage() *facets.Lineage
}

type OwnershipMetadata interface {
	GetOwnership() *facets.Ownership
}
