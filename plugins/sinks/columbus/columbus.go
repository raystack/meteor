package columbus

type Record struct {
	Urn         string            `json:"urn"`
	Name        string            `json:"name"`
	Service     string            `json:"service"`
	Upstreams   []LineageRecord   `json:"upstreams"`
	Downstreams []LineageRecord   `json:"downstreams"`
	Owners      []Owner           `json:"owners"`
	Description string            `json:"description"`
	Data        interface{}       `json:"data"`
	Labels      map[string]string `json:"labels"`
}

type LineageRecord struct {
	Urn  string `json:"urn"`
	Type string `json:"type"`
}

type Owner struct {
	URN   string `json:"urn"`
	Name  string `json:"name"`
	Role  string `json:"role"`
	Email string `json:"email"`
}
