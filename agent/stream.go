package agent

import (
	"fmt"
	"sync"

	"github.com/goto/meteor/models"
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
	onCloses    []func()
	mu          sync.Mutex
	shutdown    bool
	closed      bool
	err         error
}

func newStream() *stream {
	return &stream{}
}

// subscribe() will register callback with a batch size to the emitter.
// Calling this will not start listening yet, use broadcast() to start sending data to subscriber.
func (s *stream) subscribe(callback func(batch []models.Record) error, batchSize int) *stream {
	s.subscribers = append(s.subscribers, &subscriber{
		callback:  callback,
		batchSize: batchSize,
		channel:   make(chan models.Record),
	})

	return s
}

// onClose() is used to register callback for after stream is closed.
func (s *stream) onClose(callback func()) *stream {
	s.onCloses = append(s.onCloses, callback)

	return s
}

// broadcast() will start listening to emitter for any pushed data.
// This process is blocking, so most times you would want to call this inside a goroutine.
func (s *stream) broadcast() error {
	var wg sync.WaitGroup
	wg.Add(len(s.subscribers))
	for _, l := range s.subscribers {
		go func(l *subscriber) {
			defer func() {
				if r := recover(); r != nil {
					s.closeWithError(fmt.Errorf("%s", r))
				}
				wg.Done()
			}()

			batch := newBatch(l.batchSize)
			// listen to channel and emit data to subscriber callback if batch is full
			for d := range l.channel {
				if err := batch.add(d); err != nil {
					s.closeWithError(err)
				}
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
		}(l)
	}

	wg.Wait()

	return s.err
}

// push() will run the record through all the registered middleware
// and emit the record to all registered subscribers.
func (s *stream) push(data models.Record) {
	data, err := s.runMiddlewares(data)
	if err != nil {
		s.closeWithError(errors.Wrap(err, "emitter: error running middleware"))
		return
	}

	for _, l := range s.subscribers {
		l.channel <- data
	}
}

// setMiddleware registers a middleware that will be used to
// process given record before broadcasting.
func (s *stream) setMiddleware(m streamMiddleware) *stream {
	s.middlewares = append(s.middlewares, m)
	return s
}

func (s *stream) closeWithError(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	s.Close()
}

func (s *stream) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.shutdown {
		return
	}

	for _, l := range s.subscribers {
		close(l.channel)
	}
	s.shutdown = true
}

// Close the emitter and signalling all subscriber of the event.
func (s *stream) Close() {
	s.Shutdown()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	for _, onClose := range s.onCloses {
		onClose()
	}
	s.closed = true
}

func (s *stream) runMiddlewares(d models.Record) (models.Record, error) {
	res := d
	for _, middleware := range s.middlewares {
		var err error
		res, err = middleware(res)
		if err != nil {
			return models.Record{}, err
		}
	}

	return res, nil
}
