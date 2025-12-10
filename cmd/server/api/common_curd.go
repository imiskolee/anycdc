package api

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	uuid "github.com/satori/go.uuid"
	"net/http"
	"time"
)

func ObjectCreate(ctx *gin.Context, name string) {
	var m map[string]interface{}
	if err := Parse(ctx, &m); err != nil {
		return
	}
	m["id"] = uuid.NewV4().String()
	m["created_at"] = time.Now()
	m["updated_at"] = time.Now()
	if err := model.DB().Table(name).Create(&m).Error; err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not create object:%s", err).Error())
		return
	}
	Success(ctx, name, m)
}

func ObjectUpdate(ctx *gin.Context, name string) {
	id := ctx.Param("id")
	if id == "" {
		Error(ctx, http.StatusBadRequest, "id is required")
		return
	}
	var m map[string]interface{}
	if err := Parse(ctx, &m); err != nil {
		return
	}
	m["updated_at"] = time.Now()
	if err := model.DB().Table(name).Where("id = ?", id).Updates(&m).Error; err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not create object:%s", err).Error())
		return
	}
	Success(ctx, "connector", m)
}

func ObjectDelete(ctx *gin.Context, name string) {
	id := ctx.Param("id")
	if id == "" {
		Error(ctx, http.StatusBadRequest, "id is required")
		return
	}
	if err := model.DB().Exec(fmt.Sprintf(`DELETE FROM "%s" WHERE id = ?`, name), id).Error; err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not delete object:%s", err).Error())
	}
	Success(ctx, name, nil)
}

func ObjectDetail(ctx *gin.Context, name string) {
	id := ctx.Param("id")
	if id == "" {
		Error(ctx, http.StatusBadRequest, "id is required")
		return
	}
	var record map[string]interface{}
	if err := model.DB().Table(name).Where("id = ?", id).Find(&record).Error; err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not get object:%s", err).Error())
	}
	Success(ctx, name, record)
}

func ObjectList(ctx *gin.Context, name string) {
	var records []map[string]interface{}
	if err := model.DB().Table(name).Order("updated_at DESC").Limit(100).Find(&records).Error; err != nil {
		Error(ctx, http.StatusInternalServerError, core.SysLogger.Errorf("can not get object:%s", err).Error())
	}
	Success(ctx, name, records)
}
