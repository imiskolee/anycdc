package postgres

import (
	"context"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
)

type Schema struct {
	opt *core.SchemaOption
}

func (s Schema) Get(dbname string, tableName string) *core.SimpleTableSchema {
	fmt.Println("Connector ID", s.opt.Connector)
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

	sql := `
SELECT
  a.attname AS column_name,
  t.typname AS data_type
FROM pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid
JOIN pg_type t ON a.atttypid = t.oid
WHERE
  c.relname = ?
  AND c.relnamespace = 'public'::regnamespace
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
`

	var fields []struct {
		ColumnName string `gorm:"column:column_name"`
		DataType   string `gorm:"column:data_type"`
	}
	if err := conn.Raw(sql, tableName).Scan(&fields).Error; err != nil {
		core.SysLogger.Error("can not get schema information, %s", connector.ID)
		return &core.SimpleTableSchema{}
	}
	ss := core.SimpleTableSchema{
		Name: tableName,
	}
	for _, field := range fields {
		ss.Fields = append(ss.Fields, core.SimpleField{
			Name: field.ColumnName,
		})
	}
	return &ss
}

func NewSchema(context context.Context, opt interface{}) core.SchemaManager {
	return &Schema{
		opt: opt.(*core.SchemaOption),
	}
}
