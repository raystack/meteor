package compass

// UpsertEntityRequest is the payload for Compass v2 UpsertEntity endpoint.
type UpsertEntityRequest struct {
	URN         string                 `json:"urn"`
	Type        string                 `json:"type"`
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Source      string                 `json:"source"`
	Properties  map[string]interface{} `json:"properties,omitempty"`
	Upstreams   []string               `json:"upstreams,omitempty"`
	Downstreams []string               `json:"downstreams,omitempty"`
}

// UpsertEdgeRequest is the payload for Compass v2 UpsertEdge endpoint.
type UpsertEdgeRequest struct {
	SourceURN string `json:"source_urn"`
	TargetURN string `json:"target_urn"`
	Type      string `json:"type"`
	Source    string `json:"source"`
}
