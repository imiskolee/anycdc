package readers

import (
	"bindolabs/anycdc/pkg/event"
	"bindolabs/anycdc/pkg/state"
)

type BatchEventSubscriber func(batch event.Batch) error

type Subscriber interface {
	Consume(event event.Event) error
}

type ReaderOptions struct {
	Subscriber  Subscriber
	StateLoader *state.State
}
