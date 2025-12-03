package mysql

import (
	"github.com/imiskolee/anycdc/pkg/common_mysql"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/entry"
	"github.com/imiskolee/anycdc/pkg/event"
	"github.com/imiskolee/anycdc/pkg/schema"
	"github.com/imiskolee/anycdc/pkg/writer"
	"github.com/imiskolee/anycdc/pkg/writer/common_rds"
	"gorm.io/gorm"
	"sync"
)

func init() {
	writer.Register(config.ConnectorTypeMySQL, NewMySQLWriter)
}

type MySQLWriter struct {
	conf   config.Writer
	schema *schema.Manager
	mutex  sync.Mutex
	conn   *gorm.DB
}

func NewMySQLWriter(conf config.Writer) writer.Writer {
	return &MySQLWriter{
		conf:   conf,
		schema: schema.NewManager(conf.Connector, common_mysql.SyncSchema),
	}
}

func (s *MySQLWriter) Prepare() error {
	connector, _ := config.GetConnector(s.conf.Connector)
	db, err := common_mysql.Connect(connector)
	if err != nil {
		return err
	}
	s.conn = db
	return nil
}

func (s *MySQLWriter) Execute(event event.Event) error {
	schema, _ := s.schema.GetTable(event.Schema, event.Table)
	newEvent := event.Copy()
	newEvent.Payload = common_rds.Convert(newEvent.Payload)
	newEvent.Payload = schema.ConvertRecord(newEvent.Payload)
	if newEvent.PrimaryKeyValue != nil {
		newEvent.PrimaryKeyValue, _ = common_rds.ConvertBuiltInTypedData(newEvent.PrimaryKeyValue.(entry.TypedData))
	}
	sql, params := writer.EventToSQL(newEvent, "`")
	return s.conn.Exec(sql, params...).Error
}
