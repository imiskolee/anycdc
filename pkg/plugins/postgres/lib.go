package postgres

import "github.com/imiskolee/anycdc/pkg/core"

const (
	pluginName = "postgres"
)

func init() {
	core.Registries.Reader.Register(pluginName, NewReader)
	core.Registries.Writer.Register(pluginName, NewWriter)
	core.Registries.Schema.Register(pluginName, NewSchema)
	core.Registries.Connector.Register(pluginName, NewConnector)
}
