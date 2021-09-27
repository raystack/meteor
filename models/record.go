package models

type Record struct {
	data Metadata
}

func NewRecord(data Metadata) Record {
	return Record{data: data}
}

func (r Record) Data() Metadata {
	return r.data
}
