package starrocks

import (
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/plugins/mysql"
)

const (
	pluginName = "starrocks"
)

func init() {
	core.RegisterPlugin(pluginName, core.Plugin{
		Name:             pluginName,
		WriterFactory:    mysql.NewWriter,
		SchemaFactory:    mysql.NewSchema,
		DumperFactory:    mysql.NewDumper,
		ConnectorFactory: mysql.NewConnector,
	})
}
