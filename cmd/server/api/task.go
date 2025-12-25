package api

import (
	"github.com/gin-gonic/gin"
	"github.com/imiskolee/anycdc/cmd/server/runtime"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"net/http"
	"os"
	"path"
)

func ActiveTask(ctx *gin.Context) {
	id := ctx.Param("id")
	var task model.Task
	if err := model.DB().Where("id = ?", id).Last(&task).Error; err != nil {
		Error(ctx, http.StatusBadRequest, core.SysLogger.Errorf("can not get task:%s", id).Error())
		return
	}
	if err := task.UpdateStatus(model.TaskStatusActive); err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not start task:%s", id).Error())
		return
	}

	if err := runtime.R.StartTask(id); err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not start task:%s", id).Error())
		return
	}
	Success(ctx, "success", true)
}

func InactiveTask(ctx *gin.Context) {
	id := ctx.Param("id")
	var task model.Task
	if err := model.DB().Where("id = ?", id).Last(&task).Error; err != nil {
		Error(ctx, http.StatusBadRequest, core.SysLogger.Errorf("can not get task:%s", id).Error())
		return
	}
	if err := task.UpdateStatus(model.TaskStatusInactive); err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not start task:%s", id).Error())
		return
	}

	if err := runtime.R.StopTask(id); err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not start task:%s", id).Error())
		return
	}
	Success(ctx, "success", true)
}

func StartTask(ctx *gin.Context) {
	id := ctx.Param("id")
	var task model.Task
	if err := model.DB().Where("id = ?", id).Last(&task).Error; err != nil {
		Error(ctx, http.StatusBadRequest, core.SysLogger.Errorf("can not get task:%s", id).Error())
		return
	}
	_ = task.UpdateCDCStatus(model.CDCStatusRunning)
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
	Success(ctx, "log", string(data))
}

func TaskRotateTo(ctx *gin.Context) {
	id := ctx.Param("id")
	var body struct {
		LastCDCPosition string `json:"last_cdc_position"`
	}
	if err := Parse(ctx, &body); err != nil {
		Error(ctx, http.StatusBadRequest, err.Error())
		return
	}
	var task model.Task
	if err := model.DB().Where("id = ?", id).Last(&task).Error; err != nil {
		Error(ctx, http.StatusBadRequest, core.SysLogger.Errorf("can not get task:%s", id).Error())
		return
	}

	if task.Status != model.TaskStatusInactive {
		Error(ctx, http.StatusNotAcceptable, "task should be inactive")
		return
	}
	task.LastCDCPosition = body.LastCDCPosition
	if err := model.DB().Save(&task).Error; err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not save task:%s", id).Error())
		return
	}
	Success(ctx, "success", true)
}

func GetTaskTableLogs(g *gin.Context) {
	id := g.Param("id")
	var tableLogs []model.TaskTable
	if err := model.DB().Where("task_id = ?", id).Find(&tableLogs).Error; err != nil {
		Error(g, http.StatusBadRequest, "can not get task logs")
		return
	}
	Success(g, "logs", tableLogs)
}

func TaskTableResync(g *gin.Context) {
	id := g.Param("id")
	var taskTable model.TaskTable
	if err := model.DB().Where("id = ?", id).Last(&taskTable).Error; err != nil {
		Error(g, http.StatusBadRequest, "can not get task table")
		return
	}
	if err := runtime.R.StopTask(taskTable.TaskID); err != nil {
		Error(g, http.StatusBadRequest, "can not stop task "+err.Error())
		return
	}
	taskTable.DumperState = model.DumperStateInitialed
	taskTable.TotalDumped = 0
	if err := model.DB().Save(&taskTable).Error; err != nil {
		Error(g, http.StatusBadRequest, "can not save task table")
		return
	}
	if err := runtime.R.StartTask(taskTable.TaskID); err != nil {
		Error(g, http.StatusBadRequest, "can not start task")
		return
	}
	Success(g, "success", true)
}
