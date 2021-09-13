package agent

import (
	"errors"

	"github.com/odpf/meteor/models"
)

type Batch struct {
	data []models.Record
	size int
}

func NewBatch(size int) *Batch {
	return &Batch{
		size: size,
	}
}

func (b *Batch) Add(d models.Record) error {
	if b.IsFull() {
		return errors.New("batch: cannot add, batch is full!")
	}

	b.data = append(b.data, d)
	return nil
}

func (b *Batch) Flush() []models.Record {
	data := b.data
	b.data = []models.Record{}

	return data
}

func (b *Batch) IsFull() bool {
	// size 0 means there is no limit, hence will not ever be full
	if b.size == 0 {
		return false
	}

	return len(b.data) >= b.size
}

func (b *Batch) IsEmpty() bool {
	return len(b.data) == 0
}
