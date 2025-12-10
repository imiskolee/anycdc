package api

import (
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

	server.POST("/:object", func(ctx *gin.Context) {
		ObjectCreate(ctx, ctx.Param("object"))
	})
	server.PUT("/:object/:id", func(ctx *gin.Context) {
		ObjectUpdate(ctx, ctx.Param("object"))
	})
	server.DELETE("/:object/:id", func(ctx *gin.Context) {
		ObjectDelete(ctx, ctx.Param("object"))
	})
	server.GET("/:object/:id", func(ctx *gin.Context) {
		ObjectDetail(ctx, ctx.Param("object"))
	})
	server.GET("/:object", func(ctx *gin.Context) {
		ObjectList(ctx, ctx.Param("object"))
	})

	server.PUT("/tasks/:id/start", StartTask)
	server.PUT("/tasks/:id/stop", StopTask)
	server.GET("/tasks/:id/logs", GetTaskLog)
	core.SysLogger.Info("Starting API Server:%s", config.G.Admin.Listen)
	if err := server.Run(config.G.Admin.Listen); err != nil {
		core.SysLogger.Error("can not start api server:%s", err)
	}
}
