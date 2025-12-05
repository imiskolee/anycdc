package entry

type Type int

type Typed interface {
	Unmarshal(v any) error
	Marshal() interface{}
}

const (
	TypeUnknown Type = iota
	TypeNumeric
	TypeString
	TypeBoolean
	TypeTimestamp
	TypeDate
	TypeTime
	TypeJSON
	TypeUUID
)

type TypedData struct {
	T Type
	V any
}

func NewTypedData(t Type, v interface{}) TypedData {
	return TypedData{
		V: v,
		T: t,
	}
}
