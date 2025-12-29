package postgres

import (
	"context"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"github.com/jackc/pgx/v5/pgtype"
)

type Schema struct {
	opt *core.SchemaOption
}

func newSchema(ctx context.Context, opt interface{}) core.SchemaManager {
	return &Schema{opt: opt.(*core.SchemaOption)}
}

func (s Schema) Get(dbname string, tableName string) *schemas.Table {
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
    attlen as attlen,
    t.typname AS data_type,
    t.oid as oid,
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
		ColumnName    string `gorm:"column:column_name"`
		DataType      string `gorm:"column:data_type"`
		IsPrimary     bool   `gorm:"column:is_primary"`
		AttNotNUll    bool   `gorm:"column:attnotnull"`
		TypeMod       int    `gorm:"column:atttypmod"`
		OID           uint32 `gorm:"column:oid"`
		ColumnDefault string `gorm:"column:column_default"`
		Attlen        int    `gorm:"column:attlen"`
	}
	if err := conn.Raw(sql, tableName).Scan(&fields).Error; err != nil {
		core.SysLogger.Error("can not get schema information for table %s, %s", tableName, err)
		return &schemas.Table{}
	}
	ss := schemas.Table{
		Name: tableName,
	}
	for _, field := range fields {
		dt, st := getBuiltInType(field.OID)
		f := schemas.Column{
			Name:         field.ColumnName,
			DataType:     dt,
			SecondlyType: st,
			IsPrimaryKey: field.IsPrimary,
			Default:      field.ColumnDefault,
			ColumnLength: field.Attlen,
		}
		if !field.AttNotNUll {
			f.Nullable = true
		}
		if field.TypeMod > 0 {
			if f.DataType == schemas.TypeString {
				f.ColumnLength = field.TypeMod - 4
			}
			if f.DataType == schemas.TypeDecimal {
				f.NumericPrecision = (field.TypeMod >> 16) & 0xFFFF
				f.NumericScale = (field.TypeMod)&0xFFFF - 4
			}
		}
		ss.Columns = append(ss.Columns, f)
	}
	return &ss
}

func (s Schema) CreateTable(table *schemas.Table) error {
	db, err := Connect(s.opt.Connector)
	if err != nil {
		return err
	}
	sqlGenerator := common_sql.NewSQLGenerator(s.opt.Connector, table, dataTypes)
	sql, err := sqlGenerator.CreateTable(table, fieldDefineBuilder)
	if err != nil {
		return err
	}
	s.opt.Logger.Info("Migrate Table SQL:%s", sql)
	return db.Exec(sql).Error
}

func fieldDefineBuilder(f schemas.Column) string {
	fieldType := fieldBuilder(f)
	nullable := ""
	if !f.Nullable {
		nullable = "NOT NULL"
	}
	return fmt.Sprintf(`"%s" %s %s`, f.Name, fieldType, nullable)
}

func fieldBuilder(f schemas.Column) string {
	switch f.DataType {
	case schemas.TypeInt:
		switch f.SecondlyType {
		case schemas.SecondlyTypeSmallInt:
			return "smallint"
		case schemas.SecondlyTypeBigInt:
			return "bigint"
		default:
			return "int"
		}
	case schemas.TypeDecimal:
		switch f.SecondlyType {
		case schemas.SecondlyTypeFloat:
			return "float"
		default:
			return fmt.Sprintf("decimal(%d,%d)", f.NumericPrecision, f.NumericScale)
		}
	case schemas.TypeString:
		switch f.SecondlyType {
		case schemas.SecondlyTypeVarChar:
			return fmt.Sprintf("varchar(%d)", f.ColumnLength)
		case schemas.SecondlyTypeChar:
			return fmt.Sprintf("char(%d)", f.ColumnLength)
		}
		return "text"
	case schemas.TypeBool:
		return "bool"
	case schemas.TypeDate:
		return "date"
	case schemas.TypeTime:
		return "time"
	case schemas.TypeTimestamp:
		if f.SecondlyType == schemas.SecondlyTypeTimestampWithTZ {
			return "timestamptz"
		}
		return "timestamp"
	case schemas.TypeJSON:
		return "json"
	}
	return "text"
}

func getBuiltInType(oid uint32) (schemas.Type, schemas.SecondlyType) {
	switch oid {
	case pgtype.Int2OID:
		return schemas.TypeInt, schemas.SecondlyTypeSmallInt
	case pgtype.Int4OID:
		return schemas.TypeInt, schemas.SecondlyTypeUnknown
	case pgtype.Int8OID:
		return schemas.TypeInt, schemas.SecondlyTypeBigInt
	case pgtype.Float4OID, pgtype.Float8OID:
		return schemas.TypeDecimal, schemas.SecondlyTypeFloat
	case pgtype.NumericOID:
		return schemas.TypeDecimal, schemas.SecondlyTypeDecimal
	case pgtype.VarcharOID:
		return schemas.TypeString, schemas.SecondlyTypeVarChar
	case pgtype.QCharOID, pgtype.BPCharOID:
		return schemas.TypeString, schemas.SecondlyTypeChar
	case pgtype.TextOID:
		return schemas.TypeString, schemas.SecondlyTypeLongText
	case pgtype.BoolOID:
		return schemas.TypeBool, schemas.SecondlyTypeUnknown
	case pgtype.UUIDOID:
		return schemas.TypeUUID, schemas.SecondlyTypeUnknown
	case pgtype.DateOID:
		return schemas.TypeDate, schemas.SecondlyTypeUnknown
	case pgtype.TimeOID:
		return schemas.TypeTime, schemas.SecondlyTypeUnknown
	case pgtype.TimestampOID:
		return schemas.TypeTimestamp, schemas.SecondlyTypeUnknown
	case pgtype.TimestamptzOID:
		return schemas.TypeTimestamp, schemas.SecondlyTypeTimestampWithTZ
	case pgtype.JSONOID, pgtype.JSONBOID:
		return schemas.TypeJSON, schemas.SecondlyTypeUnknown
	}
	return schemas.TypeUnknown, schemas.SecondlyTypeUnknown
}
