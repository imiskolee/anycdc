package mysql

import (
	"fmt"
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
)

func init() {
	writer.Register(config.ConnectorTypeMySQL, NewWriter)
}

type Writer struct {
	conf   config.Writer
	schema *schema.Manager
	mutex  sync.Mutex
	conn   *gorm.DB
}

func NewWriter(conf config.Writer) writer.Writer {
	return &Writer{
		conf:   conf,
		schema: schema.NewManager(conf.Connector, common.SyncSchema),
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
	db, err := common.ConnectMySQL(connector)
	if err != nil {
		return err
	}
	s.conn = db
	return nil
}

func (s *Writer) Execute(event event.Event) error {
	connector, _ := config.GetConnector(s.conf.Connector)
	schema, err := s.schema.GetTable(connector.Database, event.Table)
	if err != nil {
		return logs.Errorf("can not get schema from source: %v", s.conf.Connector)
	}
	newEvent := event.Copy()
	newEvent.Payload = common_rds.Convert(newEvent.Payload)
	newEvent.Payload = schema.ConvertRecord(newEvent.Payload)
	if newEvent.PrimaryKeyValue != nil {
		newEvent.PrimaryKeyValue, _ = common_rds.ConvertBuiltInTypedData(newEvent.PrimaryKeyValue.(entry.TypedData))
	}
	sql, params := writer.EventToSQL(newEvent, "`")
	fmt.Println("MySQL", sql, params)
	return s.conn.Exec(sql, params...).Error
}

func (s *Writer) Conf() config.Writer {
	return s.conf
}
