package upstream

type Resource struct {
	Project string
	Dataset string
	Name    string
}

func (r Resource) URN() string {
	return r.Project + "." + r.Dataset + "." + r.Name
}

func UniqueFilterResources(input []Resource) []Resource {
	ref := make(map[string]Resource)
	for _, i := range input {
		key := i.URN()
		ref[key] = i
	}

	var output []Resource
	for _, r := range ref {
		output = append(output, r)
	}

	return output
}
