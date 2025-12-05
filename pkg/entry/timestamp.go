package entry

import (
	"errors"
	"fmt"
	"time"
)

const (
	timestampLayout = "2006-01-02 15:04:05"
)

type Timestamp string

func (s *Timestamp) Unmarshal(v interface{}) error {
	switch v := v.(type) {
	case string:
		*s = Timestamp(v)
		break
	case *string:
		*s = Timestamp(*v)
		break
	case time.Time:
		*s = Timestamp(v.UTC().Format(timestampLayout))
		break
	case *time.Time:
		*s = Timestamp(v.UTC().Format(timestampLayout))
		break
	default:
		*s = Timestamp(fmt.Sprint(v))
	}
	return errors.New("can not unmarshal timestamp")
}
