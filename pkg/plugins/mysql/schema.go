package mysql

import (
	"context"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"strings"
	"time"
)

type schema struct {
	opt *core.SchemaOption
}

func NewSchema(context context.Context, opt interface{}) core.SchemaManager {
	return &schema{opt: opt.(*core.SchemaOption)}

}

func (s *schema) Get(dbname string, tableName string) *core.SimpleTableSchema {
	conn, err := Connect(s.opt.Connector)
	if err != nil {
		s.opt.Logger.Error("can not connect to db:%s %v", s.opt.Connector, err)
		return nil
	}

	sql := `SELECT 
 	COLUMN_KEY column_key,
        ordinal_position idx,
        IS_NULLABLE is_nullable,
  		COLUMN_NAME column_name,
  		COLUMN_TYPE column_type,
  		DATA_TYPE data_type
  		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`

	var fields []struct {
		ColumnKey  string `gorm:"column:column_key"`
		Index      uint   `gorm:"column:idx"`
		ColumnName string `gorm:"column:column_name"`
		DataType   string `gorm:"column:data_type"`
		ColumnType string `gorm:"column:column_type"`
		IsNullable string `gorm:"column:is_nullable"`
	}
	if err := conn.Raw(sql, dbname, tableName).Scan(&fields).Error; err != nil {
		s.opt.Logger.Error("failed sync schema,err=%s", err)
		return nil
	}
	sch := core.SimpleTableSchema{
		Name:       tableName,
		LastSyncAt: time.Now(),
	}
	for _, field := range fields {
		isPri := false
		if strings.ToUpper(field.ColumnKey) == "PRI" {
			isPri = true
		}
		sch.Fields = append(sch.Fields, core.SimpleField{
			Name:         field.ColumnName,
			Index:        field.Index - 1, //starting from 1 on data tables
			IsPrimaryKey: isPri,
			Type:         field.DataType,
			RawDataType:  field.ColumnType,
			Nullable:     field.IsNullable == "YES",
		})
	}
	return &sch
}

func (s *schema) CreateTable(database string, table *core.SimpleTableSchema) error {
	db, err := Connect(s.opt.Connector)
	if err != nil {
		return err
	}
	sqlGenerator := common_sql.NewSQLGenerator(s.opt.Connector, table, dataTypes)
	sql, err := sqlGenerator.CreateTable(table)
	if err != nil {
		return err
	}
	s.opt.Logger.Info("Migrate Table SQL:%s", sql)
	return db.Exec(sql).Error
}
