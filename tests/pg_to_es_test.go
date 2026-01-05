package tests

import (
	"github.com/google/uuid"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/imiskolee/anycdc/pkg/plugins/postgres"
	"testing"
	"time"
)

func TestPGToES(t *testing.T) {
	readerConnector, err := model.GetConnectorByName("test_pg_1")
	if err != nil {
		t.Fatal(err)
	}
	writerConnector, err := model.GetConnectorByName("test_es_1")
	readerDB, err := postgres.Connect(readerConnector)
	if err != nil {
		t.Fatal(err)
	}
	_ = readerDB.AutoMigrate(&BasicType{})

	taskName := "test_pg_to_es"
	tt, err := model.GetTaskByName(taskName)
	if err == nil {
		model.DB().Delete(tt)
	}
	task := model.Task{}
	task.ID = uuid.New().String()
	task.Name = taskName
	task.Reader = readerConnector.ID
	task.Writer = writerConnector.ID
	task.Tables = "basic_types"
	task.BatchSize = 100
	task.Status = model.TaskStatusActive
	task.DumperEnabled = true
	task.CDCEnabled = true
	task.DebugEnabled = true
	task.MigrateEnabled = false
	model.DB().Create(&task)

	for i := 0; i < 1; i++ {
		data := GenerateRandomBasicType()
		readerDB.Create(data)
	}
	coreTask := core.NewTask(task.ID)
	if err := coreTask.Prepare(); err != nil {
		t.Fatal(err)
	}
	go (func() {
		coreTask.Start()
	})()
	time.Sleep(2 * time.Second)
	for i := 0; i < 1; i++ {
		data := GenerateRandomBasicType()
		readerDB.Create(data)
	}
	time.Sleep(2 * time.Second)
	_ = coreTask.Stop()
	time.Sleep(10 * time.Second)

}
