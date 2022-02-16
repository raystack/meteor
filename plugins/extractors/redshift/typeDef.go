package redshift

type Table struct {
	Name   string `json:"name"`
	Schema string `json:"schema"`
	Type   string `json:"type"`
}

type ColumnList struct {
	ColumnDefault   string      `json:"columnDefault"`
	IsCaseSensitive interface{} `json:"isCaseSensitive"`
	IsCurrency      interface{} `json:"isCurrency"`
	IsSigned        interface{} `json:"isSigned"`
	Label           string      `json:"label"`
	Length          interface{} `json:"length"`
	Name            string      `json:"name"`
	Nullable        interface{} `json:"nullable"`
	Precision       interface{} `json:"precision"`
	Scale           interface{} `json:"scale"`
	SchemaName      string      `json:"schemaName"`
	TableName       string      `json:"tableName"`
	TypeName        string      `json:"typeName"`
}
