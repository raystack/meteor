package auditlog

import "cloud.google.com/go/logging/logadmin"

type InitOption func(*AuditLog)

func InitWithClient(c *logadmin.Client) func(*AuditLog) {
	return func(al *AuditLog) {
		al.client = c
	}
}

func InitWithConfig(cfg Config) func(*AuditLog) {
	return func(al *AuditLog) {
		al.config = cfg
	}
}
