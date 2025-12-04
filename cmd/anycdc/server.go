package main

import (
	"encoding/json"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/logs"
	"io"
	"net/http"
	"os"
)

const (
	CMDTaskStart  = "task_start"
	CMDTaskStop   = "task_stop"
	CMDTaskReload = "task_reload"
)

type Command struct {
	CMD  string `json:"cmd"`
	Task string `json:"task"`
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	data := map[string]string{
		"error": err.Error(),
	}
	j, _ := json.Marshal(data)
	_, _ = w.Write(j)
}

func writeSuccess(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	data := map[string]interface{}{
		"success": true,
	}
	j, _ := json.Marshal(data)
	_, _ = w.Write(j)
}

func handler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, logs.Errorf("can not read body:%s", err))
		return
	}
	var cmd Command
	if err := json.Unmarshal(body, &cmd); err != nil {
		writeError(w, logs.Errorf("can not parse body:%s", err))
		return
	}
	handlers := map[string]func(command Command) error{
		CMDTaskStart:  handleTaskStart,
		CMDTaskStop:   handleTaskStop,
		CMDTaskReload: handleTaskReload,
	}
	if h, ok := handlers[cmd.CMD]; ok {
		err := h(cmd)
		if err != nil {
			writeError(w, err)
		}
	}
	writeSuccess(w)
}

func handleTaskStart(cmd Command) error {
	return R.TaskStart(cmd.Task)
}

func handleTaskStop(cmd Command) error {
	return R.TaskStop(cmd.Task)
}

func handleTaskReload(cmd Command) error {
	return R.TaskReload(cmd.Task)
}

func InitWebServer() {
	svc := http.NewServeMux()
	svc.HandleFunc("/admin/ctl", handler)
	go func() {
		err := http.ListenAndServe(config.G.Base.Admin.Listen, svc)
		if err != nil {
			logs.Error("can not start admin api server: %v", err)
			os.Exit(1)
		}
	}()
}
