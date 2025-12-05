package types

import (
	"encoding/json"
)

type JSON interface{}

func jsonDecode(v interface{}) (JSON, error) {
	switch v := v.(type) {
	case []byte:
		var vv interface{}
		if err := json.Unmarshal(v, &vv); err != nil {
			return nil, err
		}
		return vv, nil
	default:
		return v, nil
	}
}

func (s Timestamp) String() interface{} {
	vv, _ := json.Marshal(s)
	return string(vv)
}
