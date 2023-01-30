package stencil

type JsonType string
type AvroType string

const (
	JsonTypeObject  JsonType = "object"
	JsonTypeString  JsonType = "string"
	JsonTypeNumber  JsonType = "number"
	JsonTypeArray   JsonType = "array"
	JsonTypeBoolean JsonType = "boolean"
	JsonTypeNull    JsonType = "null"

	AvroTypeNull    AvroType = "null"
	AvroTypeBoolean AvroType = "boolean"
	AvroTypeInteger AvroType = "int"
	AvroTypeLong    AvroType = "long"
	AvroTypeFloat   AvroType = "float"
	AvroTypeDouble  AvroType = "double"
	AvroTypeBytes   AvroType = "bytes"
	AvroTypeString  AvroType = "string"
	AvroTypeRecord  AvroType = "record"
	AvroTypeArray   AvroType = "array"
	AvroTypeMap     AvroType = "map"
)

type JsonSchema struct {
	Id         string                  `json:"$id"`
	Schema     string                  `json:"$schema"`
	Title      string                  `json:"title"`
	Type       JsonType                `json:"type"`
	Properties map[string]JsonProperty `json:"properties"`
}

type JsonProperty struct {
	Type        []JsonType `json:"type"`
	Description string     `json:"description"`
}

type AvroSchema struct {
	Type      string       `json:"type"`
	Namespace string       `json:"namespace"`
	Name      string       `json:"name"`
	Fields    []AvroFields `json:"fields"`
}

type AvroFields struct {
	Name string      `json:"name"`
	Type interface{} `json:"type"`
}
