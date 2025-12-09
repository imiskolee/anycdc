package types

import (
	"database/sql"
	"time"
)

var s sql.Null[time.Time]

type Optional[T any] struct {
	Type  Type
	Value T
	Valid bool
}
