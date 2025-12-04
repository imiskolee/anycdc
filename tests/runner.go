package main

import (
	"flag"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/common"
	"github.com/imiskolee/anycdc/pkg/config"
	"os"
	"path/filepath"
)

func main() {
	var rootDir string
	var connector string
	var sql string
	flag.StringVar(&rootDir, "config-dir", "./", "root config dir")
	flag.StringVar(&connector, "connector", "", "connector name")
	flag.StringVar(&sql, "sql", "", "sql name")
	flag.Parse()
	fmt.Println("Running test tool on connector:" + connector)
	err := config.Parse(rootDir)
	if err != nil {
		panic(err)
	}
	if !filepath.IsAbs(sql) {
		pwd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		sql = filepath.Join(pwd, sql)
	}

	c, err := config.GetConnector(connector)
	if err != nil {
		panic(err)
	}
	db, err := common.Connect(c)
	if err != nil {
		panic(err)
	}
	s, err := os.ReadFile(sql)
	if err != nil {
		panic(err)
	}
	rawDB, err := db.DB()
	if err != nil {
		panic(err)
	}
	_, err = rawDB.Exec(string(s))
	if err != nil {
		panic(err)
	}
}
