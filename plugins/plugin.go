package plugins

import "context"

type Extractor interface {
	Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) (err error)
}

type Processor interface {
	Process(ctx context.Context, config map[string]interface{}, in <-chan interface{}, out chan<- interface{}) (err error)
}

type Syncer interface {
	Sink(ctx context.Context, config map[string]interface{}, in <-chan interface{}) (err error)
}
