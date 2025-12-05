package types

import (
	"fmt"
	"strconv"
)

type Uint uint64

func uintDecode(v interface{}) (Uint, error) {
	switch v := v.(type) {
	case int8:
		return Uint(v), nil
	case int16:
		return Uint(v), nil
	case int32:
		return Uint(v), nil
	case int64:
		return Uint(v), nil
	case int:
		return Uint(v), nil
	case uint8:
		return Uint(v), nil
	case uint16:
		return Uint(v), nil
	case uint32:
		return Uint(v), nil
	case uint64:
		return Uint(v), nil
	case uint:
		return Uint(v), nil
	default:
		vv, err := strconv.ParseUint(fmt.Sprint(v), 10, 64)
		if err != nil {
			return 0, err
		}
		return Uint(vv), nil
	}
}

func (s *Uint) Marshal() interface{} {
	return *s
}
