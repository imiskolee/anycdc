package postgres

import (
	"context"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"strings"
)

type Schema struct {
	opt *core.SchemaOption
}

func newSchema(ctx context.Context, opt interface{}) core.SchemaManager {
	return &Schema{opt: opt.(*core.SchemaOption)}
}

func (s Schema) Get(dbname string, tableName string) *core.SimpleTableSchema {
	conn, err := Connect(s.opt.Connector)
	if err != nil {
		s.opt.Logger.Error("can not connect to db:%s %v", s.opt.Connector, err)
		return nil
	}

	sql := `
SELECT
  pa.attname AS column_name,
    pa.attnotnull as attnotnull,
    atttypmod as atttypmod,
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
		AttNotNUll bool   `gorm:"column:attnotnull"`
		TypeMod    int    `gorm:"column:atttypmod"`
	}
	if err := conn.Raw(sql, tableName).Scan(&fields).Error; err != nil {
		core.SysLogger.Error("can not get schema information for table %s, %s", tableName, err)
		return &core.SimpleTableSchema{}
	}
	ss := core.SimpleTableSchema{
		Name: tableName,
	}
	for _, field := range fields {
		f := core.SimpleField{
			Name:         field.ColumnName,
			Type:         field.DataType,
			RawDataType:  field.DataType,
			IsPrimaryKey: field.IsPrimary,
		}
		if !field.AttNotNUll {
			f.Nullable = true
		}
		if field.TypeMod > 0 {
			if strings.Contains(field.DataType, "char") || strings.Contains(field.DataType, "text") || strings.Contains(field.DataType, "byte") {
				f.ColumnLength = field.TypeMod - 4
			} else if strings.Contains(field.DataType, "float") || strings.Contains(field.DataType, "numeric") ||
				strings.Contains(field.DataType, "decimal") {
				f.NumericPrecision = (field.TypeMod >> 16) & 0xFFFF
				f.NumericScale = (field.TypeMod)&0xFFFF - 4
			}
		}
		ss.Fields = append(ss.Fields, f)
	}
	return &ss
}

func (s Schema) CreateTable(database string, table *core.SimpleTableSchema) error {
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
