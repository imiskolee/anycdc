package mysql

import "github.com/imiskolee/anycdc/pkg/core"

const (
	PluginMySQL = "mysql"
)

func init() {
	core.Registries.Reader.Register(PluginMySQL, NewReader)
	core.Registries.Writer.Register(PluginMySQL, NewWriter)
	core.Registries.Schema.Register(PluginMySQL, NewSchema)
}
