package agent

import (
	"errors"

	"github.com/goto/meteor/models"
)

// batch contains the configuration for a batch
type batch struct {
	data     []models.Record
	capacity int
}

// newBatch returns a new batch
func newBatch(capacity int) *batch {
	return &batch{
		capacity: capacity,
	}
}

// add appends a record to the batch
func (b *batch) add(d models.Record) error {
	if b.isFull() {
		return errors.New("batch: cannot add, batch is full")
	}

	b.data = append(b.data, d)
	return nil
}

// flush removes all records from the batch
func (b *batch) flush() []models.Record {
	data := b.data
	b.data = []models.Record{}

	return data
}

// isFull returns true if the batch is full
func (b *batch) isFull() bool {
	// size 0 means there is no limit, hence will not ever be full
	if b.capacity == 0 {
		return false
	}

	return len(b.data) >= b.capacity
}

// isEmpty returns true if the batch is empty
func (b *batch) isEmpty() bool {
	return len(b.data) == 0
}
