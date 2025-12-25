package tests

import (
	"bytes"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/model"
	uuid "github.com/satori/go.uuid"
)

var init_connectors = []model.Connector{
	model.Connector{
		Type:     "postgres",
		Name:     "test_pg_1",
		Host:     "127.0.0.1",
		Port:     15432,
		Username: "root",
		Password: "anycdc",
		Database: "anycdc_test",
	},
	model.Connector{
		Type:     "postgres",
		Name:     "test_pg_2",
		Host:     "127.0.0.1",
		Port:     15433,
		Username: "root",
		Password: "anycdc",
		Database: "anycdc_test",
	},

	model.Connector{
		Type:     "mysql",
		Name:     "test_mysql_1",
		Host:     "127.0.0.1",
		Port:     23306,
		Username: "root",
		Password: "anycdc",
		Database: "anycdc_test",
	},

	model.Connector{
		Type:     "mysql",
		Name:     "test_mysql_2",
		Host:     "127.0.0.1",
		Port:     23307,
		Username: "root",
		Password: "anycdc",
		Database: "anycdc_test",
	},
}

func upsert(connector model.Connector) {
	var c model.Connector
	if err := model.DB().Where("name = ?", connector.Name).First(&c).Error; err != nil {
		connector.ID = uuid.NewV4().String()
		model.DB().Create(&connector)
	}
}

func initConnectors() {
	for _, conn := range init_connectors {
		upsert(conn)
	}
}

func initialConfig() {
	configYaml := `
data_dir: .
tester: true
admin:
  listen: :8971
  database:
    type: postgres
    host: 127.0.0.1
    port: 15600
    username: root
    password: anycdc
    database: anycdc_test
`
	reader := bytes.NewReader([]byte(configYaml))
	config.ParseFromReader(reader)
}

func initialModel() {
	model.Init()
	model.ApplyMigration()
}

func init() {
	initialConfig()
	initialModel()
	initConnectors()
}
