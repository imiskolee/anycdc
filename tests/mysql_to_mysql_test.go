package tests

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/imiskolee/anycdc/pkg/plugins/mysql"
	"testing"
	"time"
)

func TestMySQLToMySQL(t *testing.T) {
	readerConnector, err := model.GetConnectorByName("test_mysql_1")
	if err != nil {
		t.Fatal(err)
	}
	writerConnector, err := model.GetConnectorByName("test_mysql_2")
	if err != nil {
		t.Fatal(err)
	}
	readerDB, err := mysql.Connect(readerConnector)
	if err != nil {
		t.Fatal(err)
	}
	writerDB, err := mysql.Connect(writerConnector)
	if err != nil {
		t.Fatal(err)
	}
	readerDB.Exec("DROP TABLE basic_types")
	writerDB.Exec("DROP TABLE basic_types")
	_ = readerDB.Debug().AutoMigrate(&BasicTypeMySQL{})
	_ = writerDB.Debug().AutoMigrate(&BasicTypeMySQL{})

	taskName := "test_mysql_to_mysql"
	tt, err := model.GetTaskByName(taskName)
	if err == nil {
		model.DB().Delete(tt)
	}
	task := model.Task{}
	task.ID = uuid.New().String()
	task.Name = "test_mysql_to_mysql"
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

	for i := 0; i < 1; i++ {
		data := GenerateRandomBasicType()
		readerDB.Create(data)
	}
	time.Sleep(60 * time.Second)
	_ = coreTask.Stop()
	var c1 int64
	var c2 int64
	readerDB.Model(&BasicType{}).Count(&c1)
	writerDB.Model(&BasicType{}).Count(&c2)
	if c1 < 1 || c2 != c1 {
		t.Fatalf("%s should be equal %d,%d", taskName, c1, c2)
	}
	fmt.Println("Test = ", c1, c2)
}
