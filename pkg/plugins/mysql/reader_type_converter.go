package mysql

import (
	"github.com/imiskolee/anycdc/pkg/core/types"
)

var typeMapping = map[types.Type][]string{
	types.TypeInt: {ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt,
		ColumnTypeBigInt},
	types.TypeDecimal: {
		ColumnTypeDecimal, ColumnTypeFloat, ColumnTypeDouble, ColumnTypeReal, ColumnTypeNumeric,
	},
	types.TypeString: {ColumnTypeVarchar, ColumnTypeChar, ColumnTypeLineString, ColumnTypeMultiLineString,
		ColumnTypeTinyText, ColumnTypeText, ColumnTypeMediumText, ColumnTypeLongText, ColumnTypeTime},
	types.TypeJSON:      {ColumnTypeJSON},
	types.TypeTimestamp: {ColumnTypeDateTime, ColumnTypeTimestamp},
	types.TypeDate:      {ColumnTypeDate},
}

func getBuiltType(mysqlType string) types.Type {
	for k, tt := range typeMapping {
		for _, t := range tt {
			if t == mysqlType {
				return k
			}
		}
	}
	return types.TypeUnknown
}
