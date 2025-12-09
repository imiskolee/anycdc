package typed_data

import (
	"errors"
	"fmt"
)

type Numeric string

func (s *Numeric) Unmarshal(v interface{}) error {
	switch v := v.(type) {
	case []byte:
		*s = Numeric(string(v))
	default:
		*s = Numeric(fmt.Sprint(v))
	}
	return errors.New("can not unmarshal Numeric")
}

func (s *Numeric) Marshal() interface{} {
	return s
}
