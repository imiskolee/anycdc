package mysql

import "github.com/imiskolee/anycdc/pkg/core"

const pluginName = "mysql"

func init() {
	core.RegisterPlugin(pluginName, core.Plugin{
		Name:             pluginName,
		ReaderFactory:    NewReader,
		WriterFactory:    NewWriter,
		SchemaFactory:    NewSchema,
		DumperFactory:    NewDumper,
		ConnectorFactory: NewConnector,
	})
}
