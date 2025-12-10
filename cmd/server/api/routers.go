package api

import (
	"github.com/gin-gonic/gin"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/core"
	"net/http"
	"path/filepath"
)

var server *gin.Engine

func init() {
	server = gin.Default()
}

func Start() {
	InitPlugins()
	server.Static("/ui", "./static")
	server.NoRoute(func(c *gin.Context) {
		// 排除 API 请求（避免 API 路由被覆盖）
		if c.Request.Method != "GET" {
			c.Status(http.StatusMethodNotAllowed)
			return
		}
		indexPath := filepath.Join("./static", "index.html")
		c.File(indexPath)
	})

	server.POST("/api/:object", func(ctx *gin.Context) {
		ObjectCreate(ctx, ctx.Param("object"))
	})
	server.PUT("/api/:object/:id", func(ctx *gin.Context) {
		ObjectUpdate(ctx, ctx.Param("object"))
	})
	server.DELETE("/api/:object/:id", func(ctx *gin.Context) {
		ObjectDelete(ctx, ctx.Param("object"))
	})
	server.GET("/api/:object/:id", func(ctx *gin.Context) {
		ObjectDetail(ctx, ctx.Param("object"))
	})
	server.GET("/api/:object", func(ctx *gin.Context) {
		ObjectList(ctx, ctx.Param("object"))
	})

	server.PUT("/api/tasks/:id/start", StartTask)
	server.PUT("/api/tasks/:id/stop", StopTask)
	server.GET("/api/tasks/:id/logs", GetTaskLog)

	core.SysLogger.Info("Starting API Server:%s", config.G.Admin.Listen)
	if err := server.Run(config.G.Admin.Listen); err != nil {
		core.SysLogger.Error("can not start api server:%s", err)
	}
}
