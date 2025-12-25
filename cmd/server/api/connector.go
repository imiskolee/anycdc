package api

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"net/http"
)

func CreateConnector(g *gin.Context) {

}

func UpdateConnector(g *gin.Context) {

}

func DeleteConnector(g *gin.Context) {

}

func GetConnector(g *gin.Context) {

}

func ListConnector(g *gin.Context) {

}

func TestConnector(g *gin.Context) {
	var connector model.Connector
	if err := Parse(g, &connector); err != nil {
		return
	}

	plugin, ok := core.GetPlugin(connector.Type)
	if !ok {
		Error(g, http.StatusBadRequest, "unsupported connector type:"+connector.Type)
		return
	}
	if plugin.ConnectorFactory == nil {
		Error(g, http.StatusBadRequest, "unsupported connector test protocol for plugin:"+connector.Type)
		return
	}
	tester := plugin.ConnectorFactory(context.Background(), &core.ConnectorOption{
		Connector: &connector,
	})
	if err := tester.Test(); err != nil {
		Error(g, http.StatusBadRequest, err.Error())
		return
	}
	Success(g, "success", true)
}
