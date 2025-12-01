package writer

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/event"
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"sync"
	"time"
)

type PostgresWriter struct {
	conf    config.Writer
	schemas map[string]SimpleTableSchema
	mutex   sync.Mutex
	conn    *gorm.DB
}

func NewPostgresWriter(conf config.Writer) *PostgresWriter {
	return &PostgresWriter{
		conf: conf,
	}
}

func (s *PostgresWriter) Prepare() error {
	connector, _ := config.GetConnector(s.conf.Connector)
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		connector.Host,
		connector.Port,
		connector.Username,
		connector.Password,
		connector.Database,
	)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		return err
	}
	s.conn = db
	return nil
}

func (s *PostgresWriter) Execute(event event.Event) error {
	if err := s.triggerSyncSchema(event.Table); err != nil {
		return err
	}
	schema := s.schemas[event.Table]
	newEvent := event.Copy()
	newEvent.Payload = schema.ConvertRecord(newEvent.Payload)
	sql, params := EventToSQL(newEvent, "\"")
	return s.conn.Exec(sql, params...).Error
}

func (s *PostgresWriter) syncSchema(tableName string) (SimpleTableSchema, error) {
	sql := `
SELECT
  a.attname AS column_name,
  t.typname AS data_type
FROM pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid
JOIN pg_type t ON a.atttypid = t.oid
WHERE
  c.relname = ?
  AND c.relnamespace = 'public'::regnamespace
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
`

	var fields []struct {
		ColumnName string `gorm:"column:column_name"`
		DataType   string `gorm:"column:data_type"`
	}
	if err := s.conn.Raw(sql, tableName).Scan(&fields).Error; err != nil {
		log.Println("Unable get information schema columns:", err.Error())
		return SimpleTableSchema{}, err
	}

	schema := SimpleTableSchema{
		Name:       tableName,
		LastSyncAt: time.Now(),
	}
	for _, field := range fields {
		schema.Fields = append(schema.Fields, SimpleField{
			Name: field.ColumnName,
		})
	}
	return schema, nil
}

func (s *PostgresWriter) triggerSyncSchema(tableName string) error {
	schema, ok := s.schemas[tableName]
	if !ok || time.Now().Sub(schema.LastSyncAt) > (10*time.Minute) {
		ss, err := s.syncSchema(tableName)
		if err != nil {
			return err
		}
		s.schemas[tableName] = ss
	}
	return nil
}
