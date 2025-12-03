package main

import (
	"flag"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/logs"
	"github.com/imiskolee/anycdc/pkg/task"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/imiskolee/anycdc/pkg/reader/mysql"
	_ "github.com/imiskolee/anycdc/pkg/reader/postgres"
	_ "github.com/imiskolee/anycdc/pkg/writer/mysql"
	_ "github.com/imiskolee/anycdc/pkg/writer/postgres"
)

func printHeader() {
	header := `
=====================================================
=================== AnyCDC ==========================
=====================================================
`
	fmt.Print(header)
}

func main() {
	printHeader()
	var tasks []*task.Task
	var rootDir string
	flag.StringVar(&rootDir, "config-dir", "./", "root config dir")
	flag.Parse()
	err := config.Parse(rootDir)
	if err != nil {
		logs.Error("load config file failed,err:", err)
		return
	}
	for _, t := range config.G.Tasks {
		tt := task.NewTask(t)
		if err := tt.Prepare(); err != nil {
			panic(err)
		}
		tasks = append(tasks, tt)
		R.wg.Add(1)
		go (func() {
			if err := tt.Start(); err != nil {
				panic(err)
			}
			R.wg.Done()
		})()
	}
	R.Tasks = tasks
	go StateSyncJob()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGHUP)
	timeout := 30 * time.Second
	select {
	case <-sigChan:
		R.Stop()
		break
	case <-time.After(timeout):
		log.Println("Focus exited after timeout")
		os.Exit(0)
	}
	R.wg.Wait()
	time.Sleep(1 * time.Second)
}
