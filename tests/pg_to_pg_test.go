package tests

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/imiskolee/anycdc/pkg/plugins/postgres"
	"testing"
	"time"
)

func TestPgToPg(t *testing.T) {
	readerConnector, err := model.GetConnectorByName("test_pg_1")
	if err != nil {
		t.Fatal(err)
	}
	writerConnector, err := model.GetConnectorByName("test_pg_2")
	if err != nil {
		t.Fatal(err)
	}
	readerDB, err := postgres.Connect(readerConnector)
	if err != nil {
		t.Fatal(err)
	}
	writerDB, err := postgres.Connect(writerConnector)
	if err != nil {
		t.Fatal(err)
	}
	_ = readerDB.AutoMigrate(&BasicType{})
	_ = writerDB.AutoMigrate(&BasicType{})
	readerDB.Exec("TRUNCATE TABLE basic_types")
	writerDB.Exec("TRUNCATE TABLE basic_types")
	taskName := "test_pg_to_pg"
	tt, err := model.GetTaskByName(taskName)
	if err == nil {
		model.DB().Delete(tt)
	}
	task := model.Task{}
	task.ID = uuid.New().String()
	task.Name = "test_pg_to_pg"
	task.Reader = readerConnector.ID
	task.Writer = writerConnector.ID
	task.Tables = "basic_types"
	task.BatchSize = 100
	task.Status = model.TaskStatusActive
	task.DumperEnabled = true
	task.CDCEnabled = true
	task.DebugEnabled = true
	task.MigrateEnabled = true
	model.DB().Create(&task)

	if task.MigrateEnabled {
		writerDB.Exec("DROP TABLE IF EXISTS basic_types")
	}

	tt = &task
	for i := 0; i < 1; i++ {
		data := GenerateRandomBasicType()
		readerDB.Create(data)
	}
	coreTask := core.NewTask(tt.ID)
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
	time.Sleep(1 * time.Second)
	var c1 int64
	var c2 int64
	readerDB.Model(&BasicType{}).Count(&c1)
	writerDB.Model(&BasicType{}).Count(&c2)
	if c1 < 1 || c2 != c1 {
		t.Fatalf("%s should be equal %d,%d", taskName, c1, c2)
	}
	fmt.Println("Test = ", c1, c2)
}

func TestInsertPGBasicData(t *testing.T) {
	readerConnector, err := model.GetConnectorByName("test_pg_1")
	if err != nil {
		t.Fatal(err)
	}
	readerDB, err := postgres.Connect(readerConnector)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100000; i++ {
		data := GenerateRandomBasicType()
		readerDB.Create(data)
	}
}
