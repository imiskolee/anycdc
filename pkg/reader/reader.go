package reader

import (
	"bindolabs/anycdc/pkg/config"
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

type Reader interface {
	Prepare() error
	Start() error
	Stop() error
}

func NewReader(conf config.Reader, opt *ReaderOptions) Reader {
	connector, _ := config.GetConnector(conf.Connector)
	switch connector.Type {
	case config.ConnectorTypePostgres:
		return NewPostgresReader(conf, *opt)
	}
	panic("invalid connector type")
}
