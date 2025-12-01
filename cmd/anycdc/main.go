package main

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/task"
	"flag"
	"sync"
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

	tg.Wait()
}
