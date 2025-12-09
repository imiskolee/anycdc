package api

import "github.com/gin-gonic/gin"

var server *gin.Engine

func init() {
	server = gin.Default()
}
