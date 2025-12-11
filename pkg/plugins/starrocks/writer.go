package starrocks

import (
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/plugins/mysql"
)

const (
	pluginName = "starrocks"
)

func init() {
	//full using mysql impl for starocks
	core.Registries.Writer.Register(pluginName, mysql.NewWriter)
	core.Registries.Connector.Register(pluginName, mysql.NewConnector)
}
