package writer

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/event"
)

type Writer interface {
	Prepare() error
	Execute(event event.Event) error
}

func NewWriter(conf config.Connector) Writer {
	switch conf.Type {
	case config.ConnectorTypeMySQL, config.ConnectorTypePostgres:
		return NewGormWriter(conf)
	}
	panic("Unsupported connector type: " + conf.Type)
}
