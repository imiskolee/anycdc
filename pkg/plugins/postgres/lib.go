package postgres

import (
	"github.com/imiskolee/anycdc/pkg/core"
)

const (
	pluginName = "postgres"
)

func init() {
	core.RegisterPlugin(pluginName, core.Plugin{
		Name:             pluginName,
		ReaderFactory:    newReader,
		WriterFactory:    NewWriter,
		SchemaFactory:    newSchema,
		DumperFactory:    newDumper,
		ConnectorFactory: newConnector,
	})
}
