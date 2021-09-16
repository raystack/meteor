package models

type Record struct {
	data interface{}
}

func NewRecord(data interface{}) Record {
	return Record{data: data}
}

func (r Record) Data() interface{} {
	return r.data
}
