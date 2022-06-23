package stencil

type JsonType string

const (
	JsonTypeObject  JsonType = "object"
	JsonTypeString  JsonType = "string"
	JsonTypeNumber  JsonType = "number"
	JsonTypeArray   JsonType = "array"
	JsonTypeBoolean JsonType = "boolean"
	JsonTypeNull    JsonType = "null"
)

type JsonSchema struct {
	Id         string              `json:"$id"`
	Schema     string              `json:"$schema"`
	Title      string              `json:"title"`
	Type       JsonType            `json:"type"`
	Properties map[string]Property `json:"properties"`
}

type Property struct {
	Type        []JsonType `json:"type"`
	Description string     `json:"description"`
}
