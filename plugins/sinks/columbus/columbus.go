package columbus

type Record struct {
	Asset       map[string]interface{} `json:"asset"`
	Upstreams   []LineageRecord        `json:"upstreams"`
	Downstreams []LineageRecord        `json:"downstreams"`
	//Urn         string                 `json:"urn"`
	//Name        string                 `json:"name"`
	//Service     string                 `json:"service"`
	//Owners      []Owner           `json:"owners"`
	//Description string            `json:"description"`
	//Data        interface{}       `json:"data"`
	//Labels      map[string]string `json:"labels"`
}

type LineageRecord struct {
	Urn     string `json:"urn"`
	Type    string `json:"type"`
	Service string `json:"service"`
}

type Owner struct {
	URN   string `json:"urn"`
	Name  string `json:"name"`
	Role  string `json:"role"`
	Email string `json:"email"`
}
