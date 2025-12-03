package common_mysql

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/schema"
	"gorm.io/gorm"
	"log"
	"strings"
	"time"
)

var conn *gorm.DB

func SyncSchema(connector config.Connector, s string, tableName string) schema.SimpleTableSchema {
	if conn == nil {
		c, err := Connect(connector)
		if err != nil {
			panic(err)
		}
		conn = c
	}
	sql := `SELECT 
    	COLUMN_KEY column_key,
        ordinal_position idx,
  		COLUMN_NAME column_name,
  		DATA_TYPE data_type 
  		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`

	var fields []struct {
		ColumnKey  string `gorm:"column:column_key"`
		Index      uint   `gorm:"column:idx"`
		ColumnName string `gorm:"column:column_name"`
		DataType   string `gorm:"column:data_type"`
	}
	if err := conn.Raw(sql, s, tableName).Scan(&fields).Error; err != nil {
		log.Println("Unable get information schema columns:", err.Error())
		return schema.SimpleTableSchema{}
	}
	sch := schema.SimpleTableSchema{
		Name:       tableName,
		LastSyncAt: time.Now(),
	}
	for _, field := range fields {
		isPri := false
		if strings.ToUpper(field.ColumnKey) == "PRI" {
			isPri = true
		}
		sch.Fields = append(sch.Fields, schema.SimpleField{
			Name:         field.ColumnName,
			Index:        field.Index - 1, //starting from 1 on data tables
			IsPrimaryKey: isPri,
			Type:         field.DataType,
		})
	}
	return sch
}
