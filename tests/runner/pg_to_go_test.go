package runner

import (
	"encoding/json"
	"fmt"
	"github.com/imiskolee/anycdc/cmd/server/runtime"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/imiskolee/anycdc/pkg/plugins/mysql"
	"github.com/imiskolee/anycdc/pkg/plugins/postgres"
	uuid "github.com/satori/go.uuid"
	"os"
	"testing"
	"time"
)

func TestPGTOPG(t1 *testing.T) {
	var t model.Task
	t.Name = "test_pg_to_pg_1"
	configFile := os.Getenv("TEST_CONFIG_FILE")
	config.Parse(configFile)
	model.Init()
	InitConnectors()
	c, _ := model.GetConnectorByName("test_pg_1")
	t.Reader = c.ID
	conn, _ := postgres.Connect(c)
	conn.Exec("DROP TABLE IF EXISTS basic_types")
	conn.AutoMigrate(&BasicType{})
	model.DB().Exec("DELETE FROM tasks")
	{
		c, _ := model.GetConnectorByName("test_pg_2")
		c2, _ := model.GetConnectorByName("test_mysql_1")
		t.Writers = fmt.Sprintf(`["%s"]`, c2.ID)
		conn, _ := postgres.Connect(c)
		conn.Exec("DROP TABLE IF EXISTS basic_types")
		conn.AutoMigrate(&BasicType{})
		conn2, _ := mysql.Connect(c2)
		conn2.Exec("DROP TABLE IF EXISTS basic_types")
		conn2.AutoMigrate(&BasicTypeMySQL{})
	}
	t.Tables = "basic_types"
	extra := map[string]interface{}{
		"publication_name": "test_pg_1",
		"slot_name":        "test_pg_slot_1",
	}
	t.Status = model.TaskStatusRunning

	j, _ := json.Marshal(extra)
	t.Extras = string(j)
	_, err := model.GetTaskByName(t.Name)
	if err != nil {
		t.ID = uuid.NewV4().String()
		model.DB().Create(&t)
	}
	_ = runtime.R.Prepare()

	for i := 0; i < 1; i++ {
		data := GenerateRandomBasicType()
		conn.Create(data)
		data.FieldBigInt = 100
		conn.Save(data)
		time.Sleep(1 * time.Second)
	}
	fmt.Println("Stopping...")
	time.Sleep(10 * time.Second)
}
