package api

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
)

var actions map[string]map[string]func(c *gin.Context) error

const (
	ActionAfterCreate  = "AfterCreate"
	ActionBeforeCreate = "BeforeCreate"
	ActionAfterUpdate  = "AfterUpdate"
	ActionBeforeUpdate = "BeforeUpdate"
	ActionAfterDelete  = "AfterDelete"
	ActionBeforeDelete = "BeforeDelete"
)

func init() {
	actions = map[string]map[string]func(c *gin.Context) error{
		"tasks": map[string]func(c *gin.Context) error{
			ActionBeforeCreate: beforeCreateTask,
			ActionBeforeDelete: beforeDeleteTask,
		},
	}
}

func runAction(c *gin.Context, name string, action string) error {
	if table, ok := actions[name]; ok {
		if h, ok := table[action]; ok {
			return h(c)
		}
	}
	return nil
}

func beforeCreateTask(c *gin.Context) error {
	var m model.Task
	if err := Parse(c, &m); err != nil {
		return err
	}
	if m.Reader == "" {
		return errors.New("empty reader")
	}
	if m.Writer == "" {
		return errors.New("empty writer")
	}
	if m.Reader == m.Writer {
		return errors.New("writer and reader cannot be the same")
	}
	return nil
}

func beforeDeleteTask(c *gin.Context) error {
	id := c.Param("id")
	var m model.Task
	if err := model.DB().Where("id = ?", id).First(&m).Error; err != nil {
		return err
	}
	if m.Status != model.TaskStatusInactive {
		return errors.New("task status should be Inactive")
	}
	var connector model.Connector
	if err := model.DB().Where("id = ?", m.Reader).Last(&connector).Error; err != nil {
		return err
	}
	reader, ok := core.GetPlugin(connector.Type)
	if !ok {
		return errors.New("connector not found")
	}
	if reader.ReaderFactory == nil {
		return nil
	}
	r := reader.ReaderFactory(context.Background(), &core.ReaderOption{
		Connector: &connector,
		Logger:    core.SysLogger,
		Task:      &m,
	})
	if err := r.Prepare(); err != nil {
		return err
	}
	if err := r.Release(); err != nil {
		return err
	}
	return nil
}
