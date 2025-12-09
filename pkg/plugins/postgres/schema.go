package postgres

import (
	"context"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
)

type Schema struct {
	opt *core.SchemaOption
}

func (s Schema) Get(dbname string, tableName string) *core.SimpleTableSchema {
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
    pa.attname AS column_name,
  t.typname AS data_type,
    CASE
        WHEN pc.contype = 'p' AND pa.attnum = ANY(pc.conkey) THEN true
        ELSE false
    END AS is_primary
FROM pg_attribute pa
JOIN pg_class pc_rel ON pa.attrelid = pc_rel.oid
JOIN pg_type t ON pa.atttypid = t.oid
LEFT JOIN pg_constraint pc
    ON pc_rel.oid = pc.conrelid
    AND pc.contype = 'p'
    AND pa.attnum = ANY(pc.conkey)
WHERE
    pc_rel.relname = ?
    AND pc_rel.relnamespace = 'public'::regnamespace
    AND pa.attnum > 0 
    AND NOT pa.attisdropped
ORDER BY pa.attnum;
`

	var fields []struct {
		ColumnName string `gorm:"column:column_name"`
		DataType   string `gorm:"column:data_type"`
		IsPrimary  bool   `gorm:"column:is_primary"`
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
			Name:         field.ColumnName,
			Type:         field.DataType,
			IsPrimaryKey: field.IsPrimary,
		})
	}
	return &ss
}

func NewSchema(context context.Context, opt interface{}) core.SchemaManager {
	return &Schema{
		opt: opt.(*core.SchemaOption),
	}
}
