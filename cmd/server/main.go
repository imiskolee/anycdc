package main

import (
	"flag"
	"github.com/imiskolee/anycdc/cmd/server/api"
	"github.com/imiskolee/anycdc/cmd/server/runtime"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/model"
	_ "github.com/imiskolee/anycdc/pkg/plugins/mysql"
	_ "github.com/imiskolee/anycdc/pkg/plugins/postgres"
)

func main() {
	var rootDir string
	flag.StringVar(&rootDir, "config", "./config.yaml", "root config dir")
	flag.Parse()
	config.Parse(rootDir)
	model.Init()
	Bootstrap()
	go runtime.R.Prepare()
	api.Start()
}
