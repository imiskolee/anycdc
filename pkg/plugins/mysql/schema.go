package mysql

import (
	"context"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"strings"
	"time"
)

type Schema struct {
	opt *core.SchemaOption
}

func NewSchema(context context.Context, opt interface{}) core.SchemaManager {
	return &Schema{opt: opt.(*core.SchemaOption)}

}

func (s *Schema) Get(dbname string, tableName string) *core.SimpleTableSchema {
	connector, err := model.GetConnectorByID(s.opt.Connector)
	if err != nil {
		s.opt.Logger.Error("can not get connector by id:%s %v", s.opt.Connector, err)
		return nil
	}
	conn, err := Connect(connector)
	if err != nil {
		s.opt.Logger.Error("can not connect to db:%s %v", s.opt.Connector, err)
		return nil
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
		})
	}
	return &sch
}
