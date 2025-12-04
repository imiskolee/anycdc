package postgres

import (
	"github.com/imiskolee/anycdc/pkg/common"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/entry"
	"github.com/imiskolee/anycdc/pkg/event"
	"github.com/imiskolee/anycdc/pkg/logs"
	"github.com/imiskolee/anycdc/pkg/schema"
	"github.com/imiskolee/anycdc/pkg/writer"
	"github.com/imiskolee/anycdc/pkg/writer/common_rds"
	"gorm.io/gorm"
	"sync"
	"time"
)

type Writer struct {
	conf    config.Writer
	schemas map[string]schema.SimpleTableSchema
	mutex   sync.Mutex
	conn    *gorm.DB
}

func init() {
	writer.Register(config.ConnectorTypePostgres, NewWriter)
}

func NewWriter(conf config.Writer) writer.Writer {
	return &Writer{
		conf:    conf,
		schemas: make(map[string]schema.SimpleTableSchema),
	}
}

func (s *Writer) Prepare() error {
	if s.conn != nil {
		db, _ := s.conn.DB()
		if db != nil {
			_ = db.Close()
		}
	}
	connector, _ := config.GetConnector(s.conf.Connector)
	db, err := common.ConnectPostgres(connector)
	if err != nil {
		return err
	}
	s.conn = db
	return nil
}

func (s *Writer) Execute(event event.Event) error {
	if err := s.triggerSyncSchema(event.Table); err != nil {
		return err
	}
	schema := s.schemas[event.Table]
	newEvent := event.Copy()
	if newEvent.PrimaryKeyValue != nil {
		newEvent.PrimaryKeyValue, _ = common_rds.ConvertBuiltInTypedData(newEvent.PrimaryKeyValue.(entry.TypedData))
	}
	newEvent.Payload = common_rds.Convert(newEvent.Payload)
	newEvent.Payload = schema.ConvertRecord(newEvent.Payload)
	sql, params := writer.EventToSQL(newEvent, "\"")

	return s.conn.Exec(sql, params...).Error
}

func (s *Writer) syncSchema(tableName string) (schema.SimpleTableSchema, error) {
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
		logs.Error("can not get schema information, %s", s.conf.Connector)
		return schema.SimpleTableSchema{}, err
	}

	ss := schema.SimpleTableSchema{
		Name:       tableName,
		LastSyncAt: time.Now(),
	}
	for _, field := range fields {
		ss.Fields = append(ss.Fields, schema.SimpleField{
			Name: field.ColumnName,
		})
	}
	return ss, nil
}

func (s *Writer) triggerSyncSchema(tableName string) error {
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

func (s *Writer) Conf() config.Writer {
	return s.conf
}
