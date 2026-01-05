package elasticsearch

import "github.com/imiskolee/anycdc/pkg/core"

const pluginName = "elasticsearch"

func init() {
	core.RegisterPlugin(pluginName, core.Plugin{
		Name:          pluginName,
		WriterFactory: newWriter,
	})
}
