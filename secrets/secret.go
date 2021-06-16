package secrets

type Secret struct {
	Name string                 `json:"name"`
	Data map[string]interface{} `json:"data"`
}

type Store interface {
	Find(name string) (Secret, error)
	Upsert(Secret) error
}
