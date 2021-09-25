package models

import (
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
)

type Metadata interface {
	GetResource() *common.Resource
	GetProperties() *facets.Properties
}
