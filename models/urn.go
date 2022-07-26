package models

import "fmt"

func TableURN(service, host, database, name string) string {
	return fmt.Sprintf("%s::%s/%s/%s", service, host, database, name)
}

func DashboardURN(service, host, id string) string {
	return fmt.Sprintf("%s::%s/%s", service, host, id)
}

func JobURN(service, host, id string) string {
	return fmt.Sprintf("%s::%s/%s", service, host, id)
}

func NewURN(service, scope, kind, id string) string {
	return fmt.Sprintf(
		"urn:%s:%s:%s:%s",
		service, scope, kind, id,
	)
}
