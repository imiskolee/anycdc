package main

import "github.com/imiskolee/anycdc/pkg/model"

func Bootstrap() {
	db := model.DB()
	if err := db.AutoMigrate(&model.Connector{}, &model.Task{}, &model.TaskLog{}, &model.SystemSetting{}, &model.Alert{}); err != nil {
		panic(err)
	}
}
