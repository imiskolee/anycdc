package tests

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/event"
	"bindolabs/anycdc/pkg/readers"
	"bindolabs/anycdc/pkg/state"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"github.com/shopspring/decimal"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
	"testing"
	"time"
)

type PGTestTable struct {
	UUID        string          `gorm:"column:uuid;primaryKey"`
	Name        string          `gorm:"column:name"`
	Description string          `gorm:"column:description"`
	Price       decimal.Decimal `gorm:"column:price"`
	Quantity    int             `gorm:"column:quantity"`
	CreatedAt   time.Time       `gorm:"column:created_at"`
	UpdatedAt   time.Time       `gorm:"column:updated_at"`
	DeletedAt   *time.Time      `gorm:"column:deleted_at"`
}

type Subscriber struct {
}

func (s *Subscriber) Consume(event event.Event) error {
	log.Println("New Event Coming", event)
	return nil
}

func (s *PGTestTable) TableName() string {
	return "table_1"
}

func TestBasicConnection(t *testing.T) {
	config.G = config.Config{
		Connectors: map[string]config.Connector{
			"db": config.Connector{
				Host:     "127.0.0.1",
				Port:     15432,
				Username: "root",
				Password: "anycdc_reader",
				Database: "postgres",
			},
		},
	}

	reader := config.Reader{
		Connector: "db",
		Tables: []string{
			"table_1",
		},
		Extras: map[string]string{
			readers.PostgresExtraSlotName:        "anycdc_slot_1",
			readers.PostgresExtraPublicationName: "anycdc_publication_1",
		},
	}

	pgReader := readers.NewPostgresReader(reader, readers.ReaderOptions{
		Subscriber:  &Subscriber{},
		StateLoader: state.NewState("001_postgres_reader_test"),
	})
	if err := pgReader.Prepare(); err != nil {
		t.Fatal(err)
	}
	go (func() {
		if err := pgReader.Start(); err != nil {
			t.Fatal(err)
		}
	})()

	conf, _ := config.GetConnector("db")
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		conf.Host,
		conf.Port,
		conf.Username,
		conf.Password,
		conf.Database,
	)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	db = db.Debug()
	db.AutoMigrate(&PGTestTable{})

	{
		for i := 0; i < 1; i++ {
			record := &PGTestTable{
				UUID:        uuid.NewV4().String(),
				Name:        fmt.Sprintf("name_%d", i),
				Description: fmt.Sprintf("description_%d", i),
				Price:       decimal.NewFromFloat(10.00001),
				Quantity:    i,
				CreatedAt:   time.Now(),
				UpdatedAt:   time.Now(),
			}
			db.Create(record)
			record.Description = fmt.Sprintf("after_changed_%d", i)
			//db.Save(record)
		}
	}

	time.Sleep(5 * time.Second)

}
