package sink

type Sink interface {
	Sink(data interface{}, config map[string]interface{}) error
}