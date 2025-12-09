package typed_data

import (
	"errors"
	"fmt"
	"strconv"
)

type Bool bool

func (s *Bool) Unmarshal(v any) error {
	switch tv := v.(type) {
	case bool:
		*s = Bool(tv)
	case *bool:
		*s = Bool(*tv)
	case Bool:
		*s = tv
	case *Bool:
		*s = *tv
	case []byte:
		vv, err := strconv.ParseBool(fmt.Sprint(string(tv)))
		if err != nil {
			return err
		}
		*s = Bool(vv)
		break
	default:
		vv, err := strconv.ParseBool(fmt.Sprint(v))
		if err != nil {
			return err
		}
		*s = Bool(vv)
		break
	}
	return errors.New("can not unmarshal to bool")
}

func (s *Bool) Marshal() interface{} {
	return s
}
