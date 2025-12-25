package common_sql

import "gorm.io/gorm"

var connections = map[string]*gorm.DB{}

func GetCachedConnection(dsn string) *gorm.DB {
	return connections[dsn]
}

func SetCachedConnection(dsn string, db *gorm.DB) {
	connections[dsn] = db
}
