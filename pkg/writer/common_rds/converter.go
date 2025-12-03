package common_rds

import (
	"bindolabs/anycdc/pkg/entry"
	"encoding/json"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"strconv"
	"time"
)

const (
	timestampLayout = "2006-01-02 15:04:05"
)

func Convert(record map[string]interface{}) map[string]interface{} {
	newRecord := make(map[string]interface{})
	for k, v := range record {
		switch t := v.(type) {
		case entry.TypedData:
			cv, err := ConvertBuiltInTypedData(t)
			if err != nil {
				panic(err)
			}
			newRecord[k] = cv
			break
		default:
			newRecord[k] = v
		}
	}
	return newRecord
}

func ConvertBuiltInTypedData(data entry.TypedData) (interface{}, error) {
	if data.V == nil {
		return nil, nil
	}
	switch data.T {
	case entry.TypeUUID:
		switch t := data.V.(type) {
		case [16]byte:
			id, err := uuid.FromBytes(t[:])
			if err != nil {
				return "", fmt.Errorf("invalid UUID: %s ", data.V)
			}
			return id.String(), nil
		}

	case entry.TypeTimestamp:
		switch t := data.V.(type) {
		case time.Time:
			return t.Format(timestampLayout), nil
		case *time.Time:
			return t.Format(timestampLayout), nil
		}
		break
	case entry.TypeBoolean:
		switch t := data.V.(type) {
		case bool:
			v := 0
			if t {
				v = 1
			}
			return fmt.Sprintf("%d", v), nil
		case string:
			b, err := strconv.ParseBool(t)
			if err != nil {
				return "", fmt.Errorf("invalid boolean: %s ", t)
			}
			v := 0
			if b {
				v = 1
			}
			return fmt.Sprintf("%d", v), nil
		}
	case entry.TypeJSON:
		j, err := json.Marshal(data.V)
		if err != nil {
			return "", fmt.Errorf("invalid JSON: %v ", data)
		}
		return string(j), nil
	}
	return fmt.Sprint(data.V), nil
}
