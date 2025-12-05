package types

import (
	"fmt"
	"strconv"
)

type Int int64

func intDecode(v interface{}) (Int, error) {
	switch v := v.(type) {
	case int8:
		return Int(v), nil
	case int16:
		return Int(v), nil
	case int32:
		return Int(v), nil
	case int64:
		return Int(v), nil
	case int:
		return Int(v), nil
	case uint8:
		return Int(v), nil
	case uint16:
		return Int(v), nil
	case uint32:
		return Int(v), nil
	case uint64:
		return Int(v), nil
	case uint:
		return Int(v), nil
	default:
		vv, err := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		if err != nil {
			return 0, err
		}
		return Int(vv), nil
	}
}

func (s *Int) Marshal() interface{} {
	return *s
}
