package mysql

import (
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func Connect(connector *model.Connector) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&multiStatements=true&interpolateParams=true&charset=utf8mb4",
		connector.Username,
		connector.Password,
		connector.Host,
		connector.Port,
		connector.Database,
	)

	cachedDB := common_sql.GetCachedConnection(dsn)
	if cachedDB != nil {
		return cachedDB, nil
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error),
	})

	if err != nil {
		core.SysLogger.Error("failed connect source database (%s), err=%s", connector, err)
		return nil, err
	}
	common_sql.SetCachedConnection(dsn, db)
	return db, nil
}
