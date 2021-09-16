package agent

import (
	"sync"

	"github.com/odpf/meteor/models"
	"github.com/pkg/errors"
)

type streamMiddleware func(src models.Record) (dst models.Record, err error)
type subscriber struct {
	callback  func([]models.Record) error
	channel   chan models.Record
	batchSize int
}

type stream struct {
	middlewares []streamMiddleware
	subscribers []*subscriber
	closed      bool
	err         error
}

func newStream() *stream {
	return &stream{}
}

// subscribe() will register callback with a batch size to the emitter.
// Calling this will not start listening yet, use broadcast() to start sending data to subscriber.
func (s *stream) subscribe(callback func(batchedData []models.Record) error, batchSize int) *stream {
	s.subscribers = append(s.subscribers, &subscriber{
		callback:  callback,
		batchSize: batchSize,
		channel:   make(chan models.Record),
	})

	return s
}

// broadcast() will start listening to emitter for any pushed data.
// This process is blocking, so most times you would want to call this inside a goroutine.
func (s *stream) broadcast() error {
	var wg sync.WaitGroup
	for _, l := range s.subscribers {
		wg.Add(1)
		go func(l *subscriber) {
			batch := newBatch(l.batchSize)
			// listen to channel and emit data to subscriber callback if batch is full
			for d := range l.channel {
				batch.add(d)
				if batch.isFull() {
					if err := l.callback(batch.flush()); err != nil {
						s.closeWithError(err)
					}
				}
			}

			// emit leftover data in the batch if any after channel is closed
			if !batch.isEmpty() {
				if err := l.callback(batch.flush()); err != nil {
					s.closeWithError(err)
				}
			}

			wg.Done()
		}(l)
	}

	wg.Wait()

	return s.err
}

// push() will run the record through all the registered middleware
// and emit the record to all registerd subscribers.
func (s *stream) push(data models.Record) {
	data, err := s.runMiddlewares(data)
	if err != nil {
		s.err = errors.Wrap(err, "emitter: error running middleware")
		s.Close()
		return
	}

	for _, l := range s.subscribers {
		l.channel <- data
	}
	return
}

// setMiddleware registers a middleware that will be used to
// process given record before broadcasting.
func (s *stream) setMiddleware(m streamMiddleware) *stream {
	s.middlewares = append(s.middlewares, m)
	return s
}

func (s *stream) closeWithError(err error) {
	s.err = err
	s.Close()
}

// Close the emitter and signalling all subscriber of the event.
func (s *stream) Close() {
	if s.closed {
		return
	}

	for _, l := range s.subscribers {
		close(l.channel)
	}
	s.closed = true
}

func (s *stream) runMiddlewares(d models.Record) (res models.Record, err error) {
	res = d
	for _, middleware := range s.middlewares {
		res, err = middleware(d)
		if err != nil {
			return
		}
	}

	return
}
