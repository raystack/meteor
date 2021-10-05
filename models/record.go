package models

// Record represents the metadata of a record
type Record struct {
	data Metadata
}

// NewRecord creates a new record
func NewRecord(data Metadata) Record {
	return Record{data: data}
}

// Data returns the record data
func (r Record) Data() Metadata {
	return r.data
}
