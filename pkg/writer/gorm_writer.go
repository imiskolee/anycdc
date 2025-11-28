package writer

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/event"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type GormWriter struct {
	conf config.Connector
	conn *gorm.DB
}

func NewGormWriter(conf config.Connector) *GormWriter {
	return &GormWriter{
		conf: conf,
	}
}

func (s *GormWriter) Prepare() error {
	switch s.conf.Type {
	case config.ConnectorTypeMySQL:
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True",
			s.conf.Username,
			s.conf.Password,
			s.conf.Host,
			s.conf.Port,
			s.conf.Database,
		)
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Info), // 打印 SQL 日志
		})
		if err != nil {
			return err
		}
		s.conn = db
	case config.ConnectorTypePostgres:
		dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			s.conf.Host,
			s.conf.Port,
			s.conf.Username,
			s.conf.Password,
			s.conf.Database,
		)
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Info),
		})
		if err != nil {
			return err
		}
		s.conn = db
	}
	s.conn = s.conn.Debug()
	return nil
}

func (s *GormWriter) Execute(event event.Event) error {
	sql, params := EventToSQL(event)
	return s.conn.Exec(sql, params...).Error
}

func (s *GormWriter) DB() *gorm.DB {
	return s.conn
}
