package stencil

type RequestPayload struct {
	Schema Schema `json:"schema"`
}

type Schema struct {
	URN         string      `json:"urn"`
	Type        string      `json:"type"`
	Name        string      `json:"name"`
	Service     string      `json:"service"`
	Description string      `json:"description"`
	Data        interface{} `json:"data"`
	Owners      []Owner     `json:"owners"`
}

type Owner struct {
	URN   string `json:"urn"`
	Name  string `json:"name"`
	Role  string `json:"role"`
	Email string `json:"email"`
}
