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
	"sync"
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
	InitWebServer()
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
		R.Tasks[tt.Conf().Path] = tt
		go (func(t string) {
			err := R.TaskStart(t)
			if err != nil {
				logs.Error("start task failed,err:", err)
			}
		})(t.Name)
	}

	go StateSyncJob()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGHUP)
	var wait sync.WaitGroup
	wait.Add(1)
	select {
	case <-sigChan:
		R.Stop()
		wait.Done()
		timeout := 30 * time.Second
		select {
		case <-time.After(timeout):
			log.Println("Focus exited after timeout")
			os.Exit(0)
		default:
		}
	}
	time.Sleep(1 * time.Second)
	wait.Wait()
}
