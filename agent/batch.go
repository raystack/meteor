package agent

import (
	"errors"

	"github.com/odpf/meteor/models"
)

type batch struct {
	data     []models.Record
	capacity int
}

func newBatch(capacity int) *batch {
	return &batch{
		capacity: capacity,
	}
}

func (b *batch) add(d models.Record) error {
	if b.isFull() {
		return errors.New("batch: cannot add, batch is full!")
	}

	b.data = append(b.data, d)
	return nil
}

func (b *batch) flush() []models.Record {
	data := b.data
	b.data = []models.Record{}

	return data
}

func (b *batch) isFull() bool {
	// size 0 means there is no limit, hence will not ever be full
	if b.capacity == 0 {
		return false
	}

	return len(b.data) >= b.capacity
}

func (b *batch) isEmpty() bool {
	return len(b.data) == 0
}
