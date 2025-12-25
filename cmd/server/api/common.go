package api

import (
	"github.com/gin-gonic/gin"
	"github.com/imiskolee/anycdc/pkg/core"
	"net/http"
)

func Parse(ctx *gin.Context, dest interface{}) error {
	if err := ctx.BindJSON(dest); err != nil {
		Error(ctx, http.StatusBadRequest, core.SysLogger.Errorf("Can not parse request body:%s", err).Error())
		return err
	}
	return nil
}

func Error(ctx *gin.Context, code int, msg ...string) {
	ctx.JSON(code, gin.H{
		"error": code,
		"msg":   msg,
	})
}

func Success(ctx *gin.Context, key string, data interface{}) {
	ctx.JSON(200, gin.H{
		"error": 0,
		"msg":   "success",
		"data": map[string]interface{}{
			key: data,
		},
	})
}
