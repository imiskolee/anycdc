package main

import (
	"github.com/imiskolee/anycdc/pkg/logs"
	"github.com/imiskolee/anycdc/pkg/task"
	"sync"
	"time"
)

type Runtime struct {
	Tasks map[string]*task.Task
	wg    sync.WaitGroup
}

var R = Runtime{
	Tasks: make(map[string]*task.Task),
}

func (r *Runtime) Stop() {
	logs.Info("starting stop...")
	for _, t := range r.Tasks {
		_ = t.Stop()
	}
}

func (r *Runtime) TaskReload(name string) error {
	t := r.getTaskByName(name)
	if t == nil {
		return logs.Errorf("can not find task by name: %s", name)
	}
	logs.Info("start reload task %s from %s", t.Conf().Name, t.Conf().Path)
	conf := t.Conf()
	if err := conf.Reload(); err != nil {
		return logs.Errorf("can not reload config: %v", err)
	}
	if t.Status == task.StatusStarted {
		if err := r.TaskStop(name); err != nil {
			return err
		}
	}
	time.Sleep(2 * time.Second) //add time buf
	newTT := task.NewTask(conf)
	R.Tasks[conf.Path] = newTT
	return r.TaskStart(newTT.Conf().Name)
}

func (r *Runtime) TaskStart(name string) error {
	t := r.getTaskByName(name)
	if t == nil {
		return logs.Errorf("can not find task by name: %s", name)
	}
	logs.Info("start task %s from %s", t.Conf().Name, t.Conf().Path)
	if err := t.Prepare(); err != nil {
		return logs.Errorf("can not prepare task:%s, err: %v", name, err)
	}
	go (func() {
		if err := t.Start(); err != nil {
			logs.Error("failed start task %s, err: %v", name, err)
			return
		}
	})()
	return nil
}

func (r *Runtime) TaskStop(name string) error {
	t := r.getTaskByName(name)
	if t == nil {
		return logs.Errorf("can not find t by name: %s", name)
	}
	logs.Info("start stop task %s from %s", t.Conf().Name, t.Conf().Path)
	return t.Stop()
}

func (r *Runtime) getTaskByName(name string) *task.Task {
	for _, t := range r.Tasks {
		if t.Conf().Name == name {
			return t
		}
	}
	return nil
}
