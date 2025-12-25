package mysql

import "github.com/imiskolee/anycdc/pkg/core/types"

var dataTypes = types.NewDefaultTypeMap()

func init() {
	dataTypes.RegisterDecoder(types.TypeBool, decodeBool)
}

func decodeBool(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case bool:
		if v {
			return 1, nil
		} else {
			return 0, nil
		}
	}
	return v, nil
}

var typeMapping = map[types.Type][]string{
	types.TypeInt:     {ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt, ColumnTypeInteger, ColumnTypeBigInt},
	types.TypeDecimal: {ColumnTypeFloat, ColumnTypeDouble, ColumnTypeReal, ColumnTypeNumeric, ColumnTypeDecimal},
	types.TypeString:  {ColumnTypeVarchar, ColumnTypeChar, ColumnTypeTinyText, ColumnTypeMediumText, ColumnTypeLongText, ColumnTypeText},
	types.TypeBlob:    {ColumnTypeTinyBlob, ColumnTypeBlob, ColumnTypeMediumBlob, ColumnTypeLongBlob},
	types.TypeJSON:    {ColumnTypeJSON},
}

func getBuiltType(typ string) types.Type {
	for k, t := range typeMapping {
		for _, tt := range t {
			if tt == typ {
				return k
			}
		}
	}
	return types.TypeUnknown
}
