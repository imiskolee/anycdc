package types

import "encoding/json"

func DefaultJSONDecoder(val interface{}) (interface{}, error) {
	switch v := val.(type) {
	case string:
		return val, nil
	case []byte:
		return string(v), nil
	}
	j, err := json.Marshal(val)
	if err != nil {
		return nil, err
	}
	return string(j), nil
}
