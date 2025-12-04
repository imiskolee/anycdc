package common

import (
	"fmt"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/logs"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func ConnectMySQL(connector config.Connector) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True",
		connector.Username,
		connector.Password,
		connector.Host,
		connector.Port,
		connector.Database,
	)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		logs.Error("failed connect source database (%s), err=%s", connector, err)
		return nil, err
	}
	return db, nil
}

func ConnectPostgres(connector config.Connector) (*gorm.DB, error) {
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
		return nil, err
	}
	return db, nil
}

func Connect(connector config.Connector) (*gorm.DB, error) {
	switch connector.Type {
	case config.ConnectorTypeMySQL:
		return ConnectMySQL(connector)
	case config.ConnectorTypePostgres:
		return ConnectPostgres(connector)
	}
	panic("Unsupported database")
}
