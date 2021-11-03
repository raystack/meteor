package models

import "fmt"

func TableUrn(service, host, database, name string) string {
	return fmt.Sprintf("%s::%s/%s/%s", service, host, database, name)
}

func DashboardUrn(service, host, id string) string {
	return fmt.Sprintf("%s::%s/%s", service, host, id)
}
