package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"strconv"
)

func DefaultIntEncoder(val interface{}) (interface{}, error) {
	switch val := val.(type) {
	case int8, int16, int32, int, int64, uint8, uint16, uint32, uint64, uint:
		return val, nil
	case string:
		v, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case []byte:
		v, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	default:
		return nil, errors.New("unsupported type")
	}
}

func DefaultUintEncoder(val interface{}) (interface{}, error) {
	switch val := val.(type) {
	case int8, int16, int32, int, int64, uint8, uint16, uint32, uint64, uint:
		return val, nil
	case string:
		v, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case []byte:
		v, err := strconv.ParseUint(string(val), 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	default:
		return nil, errors.New("unsupported type")
	}
}

func DefaultBoolEncoder(val interface{}) (interface{}, error) {
	switch val := val.(type) {
	case bool:
		return val, nil
	case string:
		v, err := strconv.ParseBool(val)
		if err != nil {
			return nil, err
		}
		return v, nil
	case []byte:
		v, err := strconv.ParseBool(string(val))
		if err != nil {
			return nil, err
		}
		return v, nil
	}
	return nil, errors.New("unsupported type")
}

func DefaultFloatEncoder(val interface{}) (interface{}, error) {
	switch val := val.(type) {
	case float32, float64:
		return val, nil
	case int8:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int:
		return float64(val), nil
	case uint8:
		return float64(val), nil
	case uint16:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case uint:
		return float64(val), nil
	case string:
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case []byte:
		v, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	}
	return nil, errors.New("unsupported type")
}

func DefaultStringEncoder(val interface{}) (interface{}, error) {
	switch val := val.(type) {
	case string:
		return val, nil
	case []byte:
		return string(val), nil
	default:
		return fmt.Sprint(val), nil
	}
}

func DefaultJSONEncoder(val interface{}) (interface{}, error) {
	switch val := val.(type) {
	case []byte:
		var dest interface{}
		if err := json.Unmarshal(val, &dest); err != nil {
			return nil, err
		}
		return dest, nil
	case string:
		var dest interface{}
		if err := json.Unmarshal([]byte(val), &dest); err != nil {
			return nil, err
		}
		return dest, nil
	}
	return val, nil
}

func DefaultDateEncoder(val interface{}) (interface{}, error) {
	return val, nil
}

func DefaultTimeEncoder(val interface{}) (interface{}, error) {
	return val, nil
}

func DefaultTimestampEncoder(val interface{}) (interface{}, error) {
	return val, nil
}

func DefaultUUIDEncoder(val interface{}) (interface{}, error) {
	switch val := val.(type) {
	case [16]byte:
		id, err := uuid.FromBytes(val[:])
		if err != nil {
			return nil, err
		}
		return id.String(), nil
	case []byte:
		id, err := uuid.FromBytes(val[:])
		if err != nil {
			return nil, err
		}
		return id.String(), nil
	}
	return val, nil
}

func DefaultBlobEncoder(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	switch val := val.(type) {
	case []byte:
		return val, nil
	case string:
		return []byte(val), nil
	}
	return val, nil
}
