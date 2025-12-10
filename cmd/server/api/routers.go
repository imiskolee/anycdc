package api

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/core"
)

var server *gin.Engine

func init() {
	server = gin.Default()
}

func Start() {
	InitPlugins()
	server.Static("/ui", "./static")

	resources := []string{
		"tasks",
		"connectors",
		"alerts",
	}
	for _, r := range resources {
		server.POST(fmt.Sprintf("/api/%s", r), func(ctx *gin.Context) {
			ObjectCreate(ctx, r)
		})
		server.PUT(fmt.Sprintf("/api/%s/:id", r), func(ctx *gin.Context) {
			ObjectUpdate(ctx, r)
		})
		server.DELETE(fmt.Sprintf("/api/%s/:id", r), func(ctx *gin.Context) {
			ObjectDelete(ctx, r)
		})
		server.GET(fmt.Sprintf("/api/%s/:id", r), func(ctx *gin.Context) {
			ObjectDetail(ctx, r)
		})
		server.GET(fmt.Sprintf("/api/%s", r), func(ctx *gin.Context) {
			ObjectList(ctx, r)
		})

	}

	server.PUT("/api/tasks/:id/start", StartTask)
	server.PUT("/api/tasks/:id/stop", StopTask)
	server.GET("/api/tasks/:id/logs", GetTaskLog)

	core.SysLogger.Info("Starting API Server:%s", config.G.Admin.Listen)
	if err := server.Run(config.G.Admin.Listen); err != nil {
		core.SysLogger.Error("can not start api server:%s", err)
	}
}
