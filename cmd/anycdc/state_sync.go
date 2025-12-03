package main

import (
	"log"
	"time"
)

func (s *Runtime) StateSync() {
	log.Println("Starting Flush State....")
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
