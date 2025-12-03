package main

import (
	"github.com/imiskolee/anycdc/pkg/logs"
	"github.com/imiskolee/anycdc/pkg/task"
	"sync"
)

type Runtime struct {
	Tasks []*task.Task
	wg    sync.WaitGroup
}

var R Runtime

func (r *Runtime) Stop() {
	logs.Info("starting stop...")
	for _, t := range r.Tasks {
		_ = t.Stop()
	}
}
