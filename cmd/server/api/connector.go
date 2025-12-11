package api

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"net/http"
)

func TestConnector(g *gin.Context) {
	var connector model.Connector
	if err := Parse(g, &connector); err != nil {
		Error(g, http.StatusBadRequest, "can not parse connector:"+err.Error())
		return
	}
	factory := core.Registries.Connector.Get(connector.Type)
	if factory == nil {
		Error(g, http.StatusBadRequest, "unsupported connector:"+connector.Type)
		return
	}

	conn := factory(context.Background(), &connector)
	if err := conn.Test(); err != nil {
		Error(g, http.StatusBadRequest, "invalid connection:"+err.Error())
		return
	}
	Success(g, "success", true)
}
