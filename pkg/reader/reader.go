package reader

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/event"
	"bindolabs/anycdc/pkg/state"
)

type Subscriber interface {
	Consume(event event.Event) error
}

type ReaderOptions struct {
	Subscriber  Subscriber
	StateLoader *state.State
}

type Reader interface {
	Prepare() error
	Start() error
	Stop() error
	Save() error
}

func NewReader(conf config.Reader, opt *ReaderOptions) Reader {
	connector, _ := config.GetConnector(conf.Connector)
	factory, ok := _readers[connector.Type]
	if !ok {
		panic("invalid connector type")
	}
	return factory(conf, opt)
}
