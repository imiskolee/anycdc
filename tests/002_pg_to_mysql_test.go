package tests

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/reader/postgres"
	"bindolabs/anycdc/pkg/task"
	"bindolabs/anycdc/pkg/writer"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"github.com/shopspring/decimal"
	"testing"
	"time"
)

func TestPGToMySQL(t *testing.T) {
	config.G = config.Config{
		Connectors: map[string]config.Connector{
			"db": config.Connector{
				Type:     config.ConnectorTypePostgres,
				Host:     "127.0.0.1",
				Port:     15432,
				Username: "root",
				Password: "anycdc_reader",
				Database: "postgres",
			},
			"mysql": config.Connector{
				Type:     config.ConnectorTypeMySQL,
				Host:     "127.0.0.1",
				Port:     13306,
				Username: "root",
				Password: "anycdc_reader",
				Database: "mysql",
			},
		},
	}

	tConf := config.Task{
		Reader: config.Reader{
			Connector: "db",
			Tables: []string{
				"table_1",
			},
			Extras: map[string]string{
				postgres.PostgresExtraSlotName:        "anycdc_slot_2",
				postgres.PostgresExtraPublicationName: "anycdc_publication_2",
			},
		},
		Writers: []config.Writer{
			config.Writer{
				Connector: "mysql",
			},
		},
	}

	tt := task.NewTask(tConf)

	if err := tt.Prepare(); err != nil {
		t.Fatal(err)
	}
	go tt.Start()

	conf, _ := config.GetConnector("db")
	w := writer.NewGormWriter(conf)
	w.Prepare()
	w.DB().AutoMigrate(&PGTestTable{})
	{
		conf, _ := config.GetConnector("mysql")
		w := writer.NewGormWriter(conf)
		w.Prepare()
		w.DB().AutoMigrate(&PGTestTable{})
	}

	{
		for i := 0; i < 10; i++ {
			record := &PGTestTable{
				UUID:        uuid.NewV4().String(),
				Name:        fmt.Sprintf("name_%d", i),
				Description: fmt.Sprintf("description_%d", i),
				Price:       decimal.NewFromFloat(10.00001),
				Quantity:    i,
				CreatedAt:   time.Now(),
				UpdatedAt:   time.Now(),
			}
			w.DB().Create(record)
			record.Description = fmt.Sprintf("after_changed_%d", i)
			w.DB().Save(record)
		}
	}
	time.Sleep(5 * time.Second)
}
