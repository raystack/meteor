package columbus

type Record struct {
	Urn         string            `json:"urn"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Service     string            `json:"service"`
	Data        interface{}       `json:"data"`
	Labels      map[string]string `json:"labels"`
}
