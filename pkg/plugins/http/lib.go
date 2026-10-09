package http

import "github.com/imiskolee/anycdc/pkg/core"

const pluginName = "http"

func init() {
	core.RegisterPlugin(pluginName, core.Plugin{
		Name:             pluginName,
		WriterFactory:    newWriter,
		ConnectorFactory: newConnector,
	})
}