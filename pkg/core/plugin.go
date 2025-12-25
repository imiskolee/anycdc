package core

import "context"

type Plugin struct {
	Name             string
	SchemaFactory    func(ctx context.Context, opt interface{}) SchemaManager
	ReaderFactory    func(ctx context.Context, opt interface{}) Reader
	WriterFactory    func(ctx context.Context, opt interface{}) Writer
	DumperFactory    func(ctx context.Context, opt interface{}) Dumper
	ConnectorFactory func(ctx context.Context, opt interface{}) Connector
}

var pluginRegistries map[string]Plugin = map[string]Plugin{}

func RegisterPlugin(name string, plugin Plugin) {
	pluginRegistries[name] = plugin
}

func GetPlugin(name string) (Plugin, bool) {
	plugin, ok := pluginRegistries[name]
	return plugin, ok
}
