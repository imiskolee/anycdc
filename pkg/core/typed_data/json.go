package typed_data

import (
	"encoding/json"
	"errors"
)

type JSON string

func (j *JSON) Unmarshal(v interface{}) error {
	switch tv := v.(type) {
	case []byte:
		*j = JSON(tv)
		break
	case string:
		*j = JSON(tv)
		break
	case *string:
		*j = JSON(*tv)
		break
	default:
		jj, err := json.Marshal(tv)
		if err != nil {
			return err
		}
		*j = JSON(string(jj))
	}
	return errors.New("can not unmarshal to JSON")
}

func (s *JSON) Marshal() interface{} {
	return s
}
