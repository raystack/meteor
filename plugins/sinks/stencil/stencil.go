package stencil

type JsonSchema struct {
	Id          string    `json:"$id"`
	Schema      string    `json:"$schema"`
	Title       string    `json:"title"`
	Type        string    `json:"type"`
	Columns     []Columns `json:"columns"`
	URN         string    `json:"urn"`
	Service     string    `json:"service"`
	Description string    `json:"description"`
}

type Columns struct {
	Profile     string `json:"profile"`
	Name        string `json:"name"`
	Properties  string `json:"properties"`
	Description string `json:"description"`
	Length      int64  `json:"length"`
	IsNullable  bool   `json:"is_nullable"`
	DataType    string `json:"dataType"`
}

type AvroSchema struct {
	Title       string        `avro:"title"`
	Columns     []AvroColumns `avro:"columns"`
	URN         string        `avro:"urn"`
	Service     string        `avro:"service"`
	Description string        `avro:"description"`
}

type AvroColumns struct {
	Profile     string `avro:"profile"`
	Name        string `avro:"name"`
	Properties  string `avro:"properties"`
	Description string `avro:"description"`
	Length      int64  `avro:"length"`
	IsNullable  bool   `avro:"is_nullable"`
	DataType    string `avro:"dataType"`
}
