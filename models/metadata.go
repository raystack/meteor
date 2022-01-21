package models

import (
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
)

// Metadata is a wrapper for the meta
type Metadata interface {
	GetResource() *commonv1beta1.Resource
	GetProperties() *facetsv1beta1.Properties
}

type LineageMetadata interface {
	GetLineage() *facetsv1beta1.Lineage
}

type OwnershipMetadata interface {
	GetOwnership() *facetsv1beta1.Ownership
}
