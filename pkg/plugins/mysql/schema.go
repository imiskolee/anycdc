package mysql

import (
	"context"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"strings"
)

type schema struct {
	opt *core.SchemaOption
}

func NewSchema(context context.Context, opt interface{}) core.SchemaManager {
	return &schema{opt: opt.(*core.SchemaOption)}

}

func (s *schema) Get(dbname string, tableName string) *schemas.Table {
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
  		DATA_TYPE data_type,
  		CHARACTER_OCTET_LENGTH column_length,
  		NUMERIC_PRECISION numeric_precision,
  		NUMERIC_SCALE numeric_scale
  		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`

	var fields []struct {
		ColumnKey        string `gorm:"column:column_key"`
		Index            uint   `gorm:"column:idx"`
		ColumnName       string `gorm:"column:column_name"`
		DataType         string `gorm:"column:data_type"`
		ColumnType       string `gorm:"column:column_type"`
		IsNullable       string `gorm:"column:is_nullable"`
		ColumnLength     string `gorm:"column:column_length"`
		NumericPrecision string `gorm:"column:numeric_precision"`
		NumericScale     string `gorm:"column:numeric_scale"`
	}
	if err := conn.Raw(sql, dbname, tableName).Scan(&fields).Error; err != nil {
		s.opt.Logger.Error("failed sync schema,err=%s", err)
		return nil
	}
	sch := schemas.Table{
		Name: tableName,
	}
	for _, field := range fields {
		isPri := false
		if strings.ToUpper(field.ColumnKey) == "PRI" {
			isPri = true
		}
		t, st := getBuiltType(field.DataType)
		sch.Columns = append(sch.Columns, schemas.Column{
			Name:         field.ColumnName,
			Index:        field.Index - 1, //starting from 1 on data tables
			IsPrimaryKey: isPri,
			DataType:     t,
			SecondlyType: st,
			Nullable:     field.IsNullable == "YES",
		})
	}
	return &sch
}

func (s *schema) CreateTable(table *schemas.Table) error {
	db, err := Connect(s.opt.Connector)
	if err != nil {
		return err
	}
	sqlGenerator := common_sql.NewSQLGenerator(s.opt.Connector, table, dataTypes)
	sql, err := sqlGenerator.CreateTable(table, getFieldDefineDescription)
	if err != nil {
		return err
	}
	s.opt.Logger.Info("Migrate Table SQL:%s", sql)
	return db.Exec(sql).Error
}

func getFieldDefineDescription(f schemas.Column) string {
	fieldType := getFieldTypeDefinition(f)
	nullable := ""
	if !f.Nullable {
		nullable = "NOT NULL"
	}
	return fmt.Sprintf("`%s` %s %s", f.Name, fieldType, nullable)
}

func getFieldTypeDefinition(f schemas.Column) string {
	switch f.DataType {
	case schemas.TypeInt:
		switch f.SecondlyType {
		case schemas.SecondlyTypeSmallInt:
			return "smallint"
		case schemas.SecondlyTypeMediumInt:
			return "mediumint"
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
		case schemas.SecondlyTypeSmallText:
			return "tinytext"
		case schemas.SecondlyTypeMediumText:
			return "mediumtext"
		case schemas.SecondlyTypeLongText:
			return "longtext"
		default:
			return "text"
		}
	case schemas.TypeBlob:
		switch f.SecondlyType {
		case schemas.SecondlyTypeSmallBlob:
			return "smallblob"
		case schemas.SecondlyTypeMediumBlob:
			return "mediumblob"
		case schemas.SecondlyTypeLongBlob:
			return "longblob"
		default:
			return "blob"
		}
	case schemas.TypeDate:
		return "date"
	case schemas.TypeTimestamp:
		return "datetime"
	case schemas.TypeTime:
		return "time"
	case schemas.TypeJSON:
		return "json"
	case schemas.TypeUUID:
		return "char(36)"
	case schemas.TypeBool:
		return "tinyint(1)"
	}
	return "text"
}
