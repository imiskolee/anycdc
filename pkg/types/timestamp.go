package types

import (
	"errors"
	"time"
)

const (
	timestampLayout = "2006-01-02 15:04:05"
)

type Timestamp time.Time

func timestampDecode(v interface{}) (Timestamp, error) {
	switch v := v.(type) {
	case time.Time:
		return Timestamp(v), nil
	case *time.Time:
		return Timestamp(*v), nil
	case int:
		return Timestamp(time.Unix(int64(v), 0)), nil
	case int64:
		return Timestamp(time.Unix(v, 0)), nil
	case uint64:
		return Timestamp(time.Unix(int64(v), 0)), nil
	case string:
		t, err := time.Parse(timestampLayout, v)
		if err != nil {
			return Timestamp(time.Unix(0, 0)), err
		}
		return Timestamp(t), nil
	}
	return Timestamp(time.Unix(0, 0)), errors.New("can not convert to Timestamp")
}

func (s Timestamp) Marshal() interface{} {
	return time.Time(s).Format(timestampLayout)
}
