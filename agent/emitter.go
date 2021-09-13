package agent

import (
	"sync"

	"github.com/odpf/meteor/models"
	"github.com/pkg/errors"
)

type EmitterMiddleware func(src models.Record) (dst models.Record, err error)
type EmitterListener struct {
	callback  func([]models.Record) error
	batchSize int
	channel   chan models.Record
}

type Emitter struct {
	middlewares []EmitterMiddleware
	listeners   []*EmitterListener
	hasEmitted  bool
	hasClosed   bool
	err         error
}

func NewEmitter() *Emitter {
	return &Emitter{}
}

// SetListener will register callback with a batch size to the emitter.
// Calling this will not start listening yet, use Listen() to start listening.
func (e *Emitter) SetListener(callback func(batchedData []models.Record) error, batchSize int) *Emitter {
	e.listeners = append(e.listeners, &EmitterListener{
		callback:  callback,
		batchSize: batchSize,
		channel:   make(chan models.Record),
	})

	return e
}

// Listen will start listening to emitter for any pushed data.
// This process is blocking, so most times you would want to call this inside a goroutine.
func (e *Emitter) Listen() error {
	if e.hasEmitted {
		return errors.New("emitter: could not set listener, message is already being pushed!")
	}

	var wg sync.WaitGroup
	for _, l := range e.listeners {
		wg.Add(1)
		go func(l *EmitterListener) {
			batch := NewBatch(l.batchSize)
			// listen to channel and emit data to listener callback if batch is full
			for d := range l.channel {
				batch.Add(d)
				if batch.IsFull() {
					if err := l.callback(batch.Flush()); err != nil {
						e.closeWithError(err)
					}
				}
			}

			// emit leftover data in the batch if any after channel is closed
			if !batch.IsEmpty() {
				if err := l.callback(batch.Flush()); err != nil {
					e.closeWithError(err)
				}
			}

			wg.Done()
		}(l)
	}

	wg.Wait()

	// there could be error when emitting
	if e.err != nil {
		return e.err
	}

	return nil
}

// Emit will run the record through all the registered middleware
// and emit the record to all registerd listeners.
func (e *Emitter) Emit(data models.Record) {
	data, err := e.runMiddlewares(data)
	if err != nil {
		e.err = errors.Wrap(err, "emitter: error running middleware")
		e.Close()
		return
	}

	for _, l := range e.listeners {
		l.channel <- data
	}
	return
}

// SetMiddleware registers a middleware that will be used to
// process given record before emitting.
func (e *Emitter) SetMiddleware(m EmitterMiddleware) *Emitter {
	e.middlewares = append(e.middlewares, m)
	return e
}

// Close the emitter and signalling all listeners of the event.
func (e *Emitter) Close() {
	if e.hasClosed {
		return
	}

	for _, l := range e.listeners {
		close(l.channel)
	}
	e.hasClosed = true
}

func (e *Emitter) runMiddlewares(d models.Record) (res models.Record, err error) {
	res = d
	for _, middleware := range e.middlewares {
		res, err = middleware(d)
		if err != nil {
			return
		}
	}

	return
}

// Close the emitter and signalling all listeners of the event.
func (e *Emitter) closeWithError(err error) {
	e.err = err
	e.Close()
}
