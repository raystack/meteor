package processor

type CustomPropertiesData interface {
	AppendCustomFields(values map[string]interface{}) error
}
