package main

import (
	"github.com/imiskolee/anycdc/pkg/logs"
	"time"
)

func (s *Runtime) StateSync() {
	logs.Info("starting flush state...")
	for _, t := range s.Tasks {
		_ = t.SaveState()
	}
}

func StateSyncJob() {
	for {
		time.Sleep(1 * time.Minute)
		R.StateSync()
	}
}
