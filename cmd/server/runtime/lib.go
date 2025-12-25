package runtime

import (
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"time"
)

var R Runtime = Runtime{
	Tasks: make(map[string]*core.Task),
}

type Runtime struct {
	Tasks map[string]*core.Task
}

func (s *Runtime) Prepare() error {
	var tasks []model.Task
	if err := model.DB().Where("status = ?", model.TaskStatusActive).Find(&tasks).Error; err != nil {
		return err
	}
	s.Tasks = make(map[string]*core.Task)
	for _, task := range tasks {
		_ = s.StartTask(task.ID)
	}
	s.StartSave()
	return nil
}

func (s *Runtime) StartTask(id string) error {
	if t, ok := s.Tasks[id]; ok {
		_ = t.Stop()
		time.Sleep(1 * time.Second)
	}
	t := core.NewTask(id)
	if err := t.Prepare(); err != nil {
		core.SysLogger.Error("can not prepare task:%s,%s", id, err)
		return err
	}
	s.Tasks[id] = t
	go (func() {
		if err := t.Start(); err != nil {
			core.SysLogger.Error("can not start task:%s,%s", id, err)
		}
	})()
	return nil
}

func (s *Runtime) StopTask(id string) error {
	if t, ok := s.Tasks[id]; ok {
		_ = t.Stop()
		delete(s.Tasks, id)
	}
	return nil
}

func (s *Runtime) save() {
	for _, task := range s.Tasks {
		_ = task.Save()
	}
}

func (s *Runtime) StartSave() {
	go (func() {
		for {
			time.Sleep(5 * time.Second)
			s.save()
		}
	})()
}
