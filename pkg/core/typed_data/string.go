package typed_data

import (
	"errors"
	"fmt"
)

type String string

func (s *String) Unmarshal(v interface{}) error {
	switch v := v.(type) {
	case string:
		*s = String(v)
		break
	case *string:
		*s = String(*v)
		break
	case []byte:
		*s = String(string(v))
	default:
		*s = String(fmt.Sprint(v))
	}
	return errors.New("can not unmarshal String")
}

func (s *String) Marshal() interface{} {
	return s
}
