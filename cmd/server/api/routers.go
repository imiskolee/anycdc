package api

import (
	"bytes"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/core"
	"io"
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

func CacheRequestBody() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 仅处理有请求体的方法（POST/PUT/PATCH等）
		if c.Request.Method == http.MethodPost || c.Request.Method == http.MethodPut || c.Request.Method == http.MethodPatch {
			// 读取原始请求体
			bodyBytes, err := io.ReadAll(c.Request.Body)
			if err != nil {
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "读取请求体失败"})
				return
			}

			// 关键点1：将读取后的字节重新写回请求体（使用NopCloser避免关闭）
			c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

			// 关键点2：将请求体缓存到gin.Context中，供后续二次Bind使用
			c.Set("cached_body", bodyBytes)
		}
		c.Next()
	}
}

func Start() {
	InitPlugins()
	server.Static("/ui", "./static")
	server.Use(CacheRequestBody())
	if config.G.Admin.Auth.Username == "" || config.G.Admin.Auth.Password == "" {
		panic("should provider basic auth username & password")
	}
	server.Use(gin.BasicAuth(gin.Accounts{
		config.G.Admin.Auth.Username: config.G.Admin.Auth.Password,
	}))
	server.NoRoute(func(c *gin.Context) {
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
