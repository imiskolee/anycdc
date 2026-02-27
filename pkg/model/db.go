package model

import (
	"fmt"
	"github.com/imiskolee/anycdc/pkg/config"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var adminDB *gorm.DB

func connectPostgres(connector config.Database) (*gorm.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		connector.Host,
		connector.Port,
		connector.Username,
		connector.Password,
		connector.Database,
	)

	level := logger.Error
	if config.G.LogLevel == "debug" {
		level = logger.Info
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(level),
	})
	if err != nil {
		return nil, err
	}
	return db, nil
}

func Init() {
	db, err := connectPostgres(config.G.Admin.Database)
	if err != nil {
		panic("Can not connect admin database:" + err.Error())
	}
	if config.G.Tester {
		db = db.Debug()
	}
	adminDB = db
}

func DB() *gorm.DB {

	return adminDB
}

func ApplyMigration() {
	_ = DB().AutoMigrate(&Connector{}, &Task{}, &TaskTable{})
}
