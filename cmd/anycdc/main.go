package main

import (
	"flag"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/task"
	"sync"

	_ "github.com/imiskolee/anycdc/pkg/reader/mysql"
	_ "github.com/imiskolee/anycdc/pkg/reader/postgres"
	_ "github.com/imiskolee/anycdc/pkg/writer/mysql"
	_ "github.com/imiskolee/anycdc/pkg/writer/postgres"
)

func main() {
	var tasks []*task.Task
	var rootDir string
	flag.StringVar(&rootDir, "config-dir", "./", "root config dir")
	flag.Parse()
	config.Parse(rootDir)
	tg := &sync.WaitGroup{}
	for _, t := range config.G.Tasks {
		tt := task.NewTask(t)
		if err := tt.Prepare(); err != nil {
			panic(err)
		}
		tasks = append(tasks, tt)
		tg.Add(1)
		go (func() {
			if err := tt.Start(); err != nil {
				panic(err)
			}
			tg.Done()
		})()
	}
	R.Tasks = tasks
	go StateSyncJob()
	tg.Wait()
}
