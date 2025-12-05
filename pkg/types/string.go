package types

import (
	"fmt"
)

type String string

func stringEncode(v interface{}) (String, error) {
	switch v := v.(type) {
	case string:
		return String(v), nil
	case *string:
		return String(*v), nil
	case []byte:
		return String(v), nil
	case nil:
		return "", nil
	default:
		return String(fmt.Sprint(v)), nil
	}
}

func (s String) Marshal() interface{} {
	return s
}
