package api

import (
	"github.com/gin-contrib/cors"
	"time"
)

func InitPlugins() {
	server.Use(cors.New(cors.Config{
		// 允许的源（* 表示所有，生产环境建议指定具体域名）
		AllowOrigins: []string{"*"},
		// 允许的请求方法
		AllowMethods: []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		// 允许的请求头
		AllowHeaders: []string{"Origin", "Content-Type", "Accept", "Authorization"},
		// 是否允许携带 Cookie
		AllowCredentials: true,
		// 预检请求（OPTIONS）的缓存时间
		MaxAge: 12 * time.Hour,
	}))
}
