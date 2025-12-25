package api

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/core"
	"net/http"
	"path/filepath"
)

var server *gin.Engine

func init() {
	getEngine()
}

func getEngine() *gin.Engine {
	if server == nil {
		server = gin.Default()
	}
	return server
}

func Start() {
	InitPlugins()
	server.Static("/ui", "./static")
	server.NoRoute(func(c *gin.Context) {
		fmt.Println(c.Request.URL.String())
		// 排除 API 请求（避免 API 路由被覆盖）
		if c.Request.Method != "GET" {
			c.Status(http.StatusMethodNotAllowed)
			return
		}
		indexPath := filepath.Join("./static", "index.html")
		c.File(indexPath)
	})

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

	server.PUT("/api/tasks/:id/active", ActiveTask)
	server.PUT("/api/tasks/:id/inactive", InactiveTask)
	server.PUT("/api/tasks/:id/start", StartTask)
	server.PUT("/api/tasks/:id/stop", StopTask)
	server.GET("/api/tasks/:id/logs", GetTaskLog)
	server.GET("/api/tasks/:id/table_logs", GetTaskTableLogs)
	server.PUT("/api/tasks/:id/rotate", TaskRotateTo)
	server.PUT("/api/task_tables/:id/resync", TaskTableResync)
	server.POST("/api/utils/test_connector", TestConnector)
	server.POST("/api/utils/log_tail", LogTailHandler)

	core.SysLogger.Info("Starting API Server:%s", config.G.Admin.Listen)
	if err := server.Run(config.G.Admin.Listen); err != nil {
		core.SysLogger.Error("can not start api server:%s", err)
	}
}
