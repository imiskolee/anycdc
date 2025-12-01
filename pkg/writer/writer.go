package writer

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/event"
)

type Writer interface {
	Prepare() error
	Execute(event event.Event) error
}

func NewWriter(conf config.Writer) Writer {
	connector, _ := config.GetConnector(conf.Connector)
	switch connector.Type {
	case config.ConnectorTypeMySQL:
		return NewMySQLWriter(conf)
	case config.ConnectorTypePostgres:
		return NewPostgresWriter(conf)
	}
	panic("Unsupported connector type: " + connector.Type)
}
