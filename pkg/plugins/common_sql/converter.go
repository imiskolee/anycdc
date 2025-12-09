package common_sql

import (
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core/types"
	"strconv"
	"time"
)

func ConvertToBuiltInTypedData(typ types.Type, data interface{}) types.TypedData {
	if data == nil {
		return types.NewNullTypedData(typ)
	}
	switch typ {
	case types.TypeInt:
		return convertInt(data)
	case types.TypeUint:
		return convertUint(data)
	case types.TypeDecimal:
		break
	case types.TypeString:
		return convertString(data)
	case types.TypeTimestamp:
		return convertTimestamp(data)
	case types.TypeDate:
		break
	case types.TypeJSON:
	default:
		return convertString(data)
	}
	return types.NewNullTypedData(typ)
}

func convertInt(data interface{}) types.TypedData {
	switch v := data.(type) {
	case int8:
		return types.NewTypedData(types.TypeInt, int64(v))
	case int16:
		return types.NewTypedData(types.TypeInt, int64(v))
	case int32:
		return types.NewTypedData(types.TypeInt, int64(v))
	case int64:
		return types.NewTypedData(types.TypeInt, int64(v))
	case int:
		return types.NewTypedData(types.TypeInt, int64(v))
	case uint8:
		return types.NewTypedData(types.TypeInt, int64(v))
	case uint16:
		return types.NewTypedData(types.TypeInt, int64(v))
	case uint32:
		return types.NewTypedData(types.TypeInt, int64(v))
	case uint64:
		return types.NewTypedData(types.TypeInt, int64(v))
	case uint:
		return types.NewTypedData(types.TypeInt, int64(v))
	case float32:
		return types.NewTypedData(types.TypeInt, int64(v))
	case float64:
		return types.NewTypedData(types.TypeInt, int64(v))
	case string:
		cv, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return types.NewNullTypedData(types.TypeInt)
		}
		return types.NewTypedData(types.TypeInt, int64(cv))

	case []byte:
		cv, err := strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return types.NewNullTypedData(types.TypeInt)
		}
		return types.NewTypedData(types.TypeInt, int64(cv))
	}
	return types.NewNullTypedData(types.TypeInt)
}

func convertUint(data interface{}) types.TypedData {
	switch v := data.(type) {
	case int8:
		return types.NewTypedData(types.TypeInt, uint64(v))
	case int16:
		return types.NewTypedData(types.TypeInt, uint64(v))
	case int32:
		return types.NewTypedData(types.TypeInt, uint64(v))
	case int64:
		return types.NewTypedData(types.TypeInt, uint64(v))
	case int:
		return types.NewTypedData(types.TypeInt, uint64(v))
	case uint8:
		return types.NewTypedData(types.TypeInt, uint64(v))
	case uint16:
		return types.NewTypedData(types.TypeInt, uint64(v))
	case uint32:
		return types.NewTypedData(types.TypeInt, uint64(v))
	case uint64:
		return types.NewTypedData(types.TypeInt, uint64(v))
	case uint:
		return types.NewTypedData(types.TypeInt, uint64(v))
	case float32:
		return types.NewTypedData(types.TypeInt, uint64(v))
	case float64:
		return types.NewTypedData(types.TypeInt, uint64(v))
	case string:
		cv, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return types.NewNullTypedData(types.TypeInt)
		}
		return types.NewTypedData(types.TypeInt, uint64(cv))

	case []byte:
		cv, err := strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return types.NewNullTypedData(types.TypeInt)
		}
		return types.NewTypedData(types.TypeInt, uint64(cv))
	}
	return types.NewNullTypedData(types.TypeInt)
}

func convertBool(data interface{}) types.TypedData {
	switch v := data.(type) {
	case bool:
		return types.NewTypedData(types.TypeBool, v)
	default:
		b, err := strconv.ParseBool(fmt.Sprint(data))
		if err != nil {
			return types.NewNullTypedData(types.TypeBool)
		}
		return types.NewTypedData(types.TypeBool, b)
	}
}

func convertString(data interface{}) types.TypedData {
	switch v := data.(type) {
	case string:
		return types.NewTypedData(types.TypeString, v)
	case []byte:
		return types.NewTypedData(types.TypeString, string(v))
	default:
		return types.NewTypedData(types.TypeString, fmt.Sprint(v))
	}
}

func convertTimestamp(data interface{}) types.TypedData {
	switch v := data.(type) {
	case time.Time:
		return types.NewTypedData(types.TypeString, v)
	case *time.Time:
		return types.NewTypedData(types.TypeString, *v)
	}
	return types.NewNullTypedData(types.TypeTimestamp)
}

func ConvertToSQL(params []interface{}) []interface{} {
	var ps []interface{}
	for _, p := range params {
		switch v := p.(type) {
		case types.TypedData:
			fmt.Println("is typedData")
			ps = append(ps, v.V)
			break
		case *types.TypedData:
			ps = append(ps, v.V)
			break
		default:
			ps = append(ps, p)
		}
	}
	return ps
}
