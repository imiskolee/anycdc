package writer

import (
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/event"
)

var _writers map[config.ConnectorType]func(writer config.Writer) Writer

func init() {
	_writers = make(map[config.ConnectorType]func(writer config.Writer) Writer)
}
func Register(t config.ConnectorType, factory func(writer config.Writer) Writer) {
	_writers[t] = factory
}

type Writer interface {
	Prepare() error
	Execute(event event.Event) error
}

func NewWriter(conf config.Writer) Writer {
	connector, _ := config.GetConnector(conf.Connector)
	factory, ok := _writers[connector.Type]
	if !ok {
		panic("Invalid connector type: " + connector.Type)
	}
	return factory(conf)
}
