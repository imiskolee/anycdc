package main

import (
	"github.com/imiskolee/anycdc/pkg/task"
	"log"
	"sync"
)

type Runtime struct {
	Tasks []*task.Task
	wg    sync.WaitGroup
}

var R Runtime

func (r *Runtime) Stop() {
	log.Println("Starting Stop...")
	for _, t := range r.Tasks {
		_ = t.Stop()
	}
}
