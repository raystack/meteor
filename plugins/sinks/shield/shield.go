package shield

type RequestPayload struct {
	Name     string                 `json:"name"`
	Email    string                 `json:"email"`
	Metadata map[string]interface{} `json:"metadata"`
}
