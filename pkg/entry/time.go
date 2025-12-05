package entry

import (
	"fmt"
	"strconv"
	"time"
)

const (
	timesLayout = "15:04:05"
)

type Time string

func (t *Time) Unmarshal(v interface{}) error {
	tt, err := strconv.ParseInt(fmt.Sprint(v), 10, 64)
	if err != nil {
		return err
	}
	*t = Time(time.Unix(tt, 0).UTC().Format(timesLayout))
	return nil
}
