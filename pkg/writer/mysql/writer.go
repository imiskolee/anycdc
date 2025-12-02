package mysql

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/entry"
	"bindolabs/anycdc/pkg/event"
	"bindolabs/anycdc/pkg/writer"
	"bindolabs/anycdc/pkg/writer/common_rds"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"sync"
	"time"
)

func init() {
	writer.Register(config.ConnectorTypeMySQL, NewMySQLWriter)
}

type MySQLWriter struct {
	conf    config.Writer
	schemas map[string]writer.SimpleTableSchema
	mutex   sync.Mutex
	conn    *gorm.DB
}

func NewMySQLWriter(conf config.Writer) writer.Writer {
	return &MySQLWriter{
		conf:    conf,
		schemas: make(map[string]writer.SimpleTableSchema),
	}
}

func (s *MySQLWriter) Prepare() error {
	connector, _ := config.GetConnector(s.conf.Connector)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True",
		connector.Username,
		connector.Password,
		connector.Host,
		connector.Port,
		connector.Database,
	)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info), // 打印 SQL 日志
	})
	if err != nil {
		return err
	}
	s.conn = db
	return nil
}

func (s *MySQLWriter) Execute(event event.Event) error {
	if err := s.triggerSyncSchema(event.Table); err != nil {
		return err
	}
	schema := s.schemas[event.Table]
	newEvent := event.Copy()
	newEvent.Payload = common_rds.Convert(newEvent.Payload)
	newEvent.Payload = schema.ConvertRecord(newEvent.Payload)
	newEvent.PrimaryKeyValue, _ = common_rds.ConvertBuiltInTypedData(newEvent.PrimaryKeyValue.(entry.TypedData))
	sql, params := writer.EventToSQL(newEvent, "`")
	return s.conn.Exec(sql, params...).Error
}

func (s *MySQLWriter) syncSchema(tableName string) (writer.SimpleTableSchema, error) {
	connector, _ := config.GetConnector(s.conf.Connector)
	sql := `SELECT 
  		COLUMN_NAME column_name,
  		DATA_TYPE data_type 
  		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`

	var fields []struct {
		ColumnName string `gorm:"column:column_name"`
		DataType   string `gorm:"column:data_type"`
	}
	if err := s.conn.Raw(sql, connector.Database, tableName).Scan(&fields).Error; err != nil {
		log.Println("Unable get information schema columns:", err.Error())
		return writer.SimpleTableSchema{}, err
	}

	schema := writer.SimpleTableSchema{
		Name:       tableName,
		LastSyncAt: time.Now(),
	}
	for _, field := range fields {
		schema.Fields = append(schema.Fields, writer.SimpleField{
			Name: field.ColumnName,
		})
	}
	return schema, nil
}

func (s *MySQLWriter) triggerSyncSchema(tableName string) error {
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
