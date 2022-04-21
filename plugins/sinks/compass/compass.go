package compass

type RequestPayload struct {
	Asset       Asset           `json:"asset"`
	Upstreams   []LineageRecord `json:"upstreams"`
	Downstreams []LineageRecord `json:"downstreams"`
}

type Asset struct {
	URN         string            `json:"urn"`
	Type        string            `json:"type"`
	Name        string            `json:"name"`
	Service     string            `json:"service"`
	Description string            `json:"description"`
	Owners      []Owner           `json:"owners"`
	Data        interface{}       `json:"data"`
	Labels      map[string]string `json:"labels"`
}

type LineageRecord struct {
	URN     string `json:"urn"`
	Type    string `json:"type"`
	Service string `json:"service"`
}

type Owner struct {
	URN   string `json:"urn"`
	Name  string `json:"name"`
	Role  string `json:"role"`
	Email string `json:"email"`
}
