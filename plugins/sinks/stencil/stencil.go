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
	IsNullable  bool   `json:"isNullable"`
	DataType    string `json:"dataType"`
}
