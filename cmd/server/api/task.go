package api

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/imiskolee/anycdc/cmd/server/runtime"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"net/http"
	"os"
	"path"
)

func StartTask(ctx *gin.Context) {
	id := ctx.Param("id")
	var task model.Task
	if err := model.DB().Where("id = ?", id).Last(&task).Error; err != nil {
		Error(ctx, http.StatusBadRequest, core.SysLogger.Errorf("can not get task:%s", id).Error())
		return
	}
	if err := task.UpdateStatus(model.TaskStatusRunning); err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not start task:%s", id).Error())
		return
	}

	if err := runtime.R.StartTask(id); err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not start task:%s", id).Error())
		return
	}
	Success(ctx, "success", true)
}

func StopTask(ctx *gin.Context) {
	id := ctx.Param("id")

	var task model.Task
	if err := model.DB().Where("id = ?", id).Last(&task).Error; err != nil {
		Error(ctx, http.StatusBadRequest, core.SysLogger.Errorf("can not get task:%s", id).Error())
		return
	}
	if err := task.UpdateStatus(model.TaskStatusStopped); err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not start task:%s", id).Error())
		return
	}

	if err := runtime.R.StopTask(id); err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not start task:%s", id).Error())
		return
	}
	Success(ctx, "success", true)
}

func GetTaskLog(ctx *gin.Context) {
	id := ctx.Param("id")
	fileName := path.Join(config.G.DataDir, "tasks", id+".log")
	data, err := os.ReadFile(fileName)
	if err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not read file:%s", id).Error())
		return
	}
	fmt.Println(string(data))
	Success(ctx, "log", string(data))
}
