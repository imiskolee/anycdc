package mysql

import (
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func Connect(connector *model.Connector) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True",
		connector.Username,
		connector.Password,
		connector.Host,
		connector.Port,
		connector.Database,
	)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error),
	})
	if err != nil {
		core.SysLogger.Error("failed connect source database (%s), err=%s", connector, err)
		return nil, err
	}
	return db, nil
}
