package utils

import (
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
)

type CustomPropertiesUpdateFn func(map[string]string) error

func ModifyCustomProperties(data interface{}, updateFn CustomPropertiesUpdateFn) error {
	switch data := data.(type) {
	case []meta.Table:
		return modifyTableCustomProperties(data, updateFn)
	case []meta.Topic:
		return modifyTopicCustomProperties(data, updateFn)
	case []meta.Dashboard:
		return modifyDashboardCustomProperties(data, updateFn)
	}

	return nil
}

func modifyTableCustomProperties(tables []meta.Table, updateFn CustomPropertiesUpdateFn) (err error) {
	for i := 0; i < len(tables); i++ {
		table := &tables[i]
		if table.Custom == nil {
			table.Custom = createCustomFacet()
		}

		err = updateFn(table.Custom.CustomProperties)
		if err != nil {
			return
		}
	}

	return
}
func modifyTopicCustomProperties(topics []meta.Topic, updateFn CustomPropertiesUpdateFn) (err error) {
	for i := 0; i < len(topics); i++ {
		topic := &topics[i]
		if topic.Custom == nil {
			topic.Custom = createCustomFacet()
		}

		err = updateFn(topic.Custom.CustomProperties)
		if err != nil {
			return
		}
	}

	return
}
func modifyDashboardCustomProperties(dashboards []meta.Dashboard, updateFn CustomPropertiesUpdateFn) (err error) {
	for i := 0; i < len(dashboards); i++ {
		dashboard := &dashboards[i]
		if dashboard.Custom == nil {
			dashboard.Custom = createCustomFacet()
		}

		err = updateFn(dashboard.Custom.CustomProperties)
		if err != nil {
			return
		}
	}

	return
}
func createCustomFacet() *facets.Custom {
	return &facets.Custom{
		CustomProperties: make(map[string]string),
	}
}
