package mysql

import (
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/core/types"
)

var dataTypes = types.NewDefaultTypeMap()

func init() {
	dataTypes.RegisterDecoder(schemas.TypeBool, decodeBool)
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

func getBuiltType(typ string) (schemas.Type, schemas.SecondlyType) {
	switch typ {
	case ColumnTypeSmallInt:
		return schemas.TypeInt, schemas.SecondlyTypeSmallInt
	case ColumnTypeMediumInt:
		return schemas.TypeInt, schemas.SecondlyTypeMediumInt
	case ColumnTypeBigInt:
		return schemas.TypeInt, schemas.SecondlyTypeBigInt
	case ColumnTypeInt:
		return schemas.TypeInt, schemas.SecondlyTypeUnknown
	case ColumnTypeFloat, ColumnTypeDouble, ColumnTypeReal:
		return schemas.TypeDecimal, schemas.SecondlyTypeFloat
	case ColumnTypeNumeric, ColumnTypeDecimal:
		return schemas.TypeDecimal, schemas.SecondlyTypeDecimal
	case ColumnTypeVarchar:
		return schemas.TypeString, schemas.SecondlyTypeVarChar
	case ColumnTypeChar:
		return schemas.TypeString, schemas.SecondlyTypeChar
	case ColumnTypeTinyText:
		return schemas.TypeString, schemas.SecondlyTypeSmallText
	case ColumnTypeMediumText:
		return schemas.TypeString, schemas.SecondlyTypeMediumText
	case ColumnTypeLongText:
		return schemas.TypeString, schemas.SecondlyTypeLongText
	case ColumnTypeText:
		return schemas.TypeString, schemas.SecondlyTypeText
	case ColumnTypeTinyBlob:
		return schemas.TypeBlob, schemas.SecondlyTypeSmallBlob
	case ColumnTypeMediumBlob:
		return schemas.TypeBlob, schemas.SecondlyTypeMediumBlob
	case ColumnTypeLongBlob:
		return schemas.TypeBlob, schemas.SecondlyTypeLongBlob
	case ColumnTypeBlob:
		return schemas.TypeBlob, schemas.SecondlyTypeBlob
	case ColumnTypeDate:
		return schemas.TypeDate, schemas.SecondlyTypeUnknown
	case ColumnTypeTime:
		return schemas.TypeTime, schemas.SecondlyTypeUnknown
	case ColumnTypeDateTime:
		return schemas.TypeTimestamp, schemas.SecondlyTypeUnknown
	case ColumnTypeJSON:
		return schemas.TypeJSON, schemas.SecondlyTypeUnknown
	}
	return schemas.TypeUnknown, schemas.SecondlyTypeUnknown
}
