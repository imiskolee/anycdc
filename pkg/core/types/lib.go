package types

type Type int

const (
	TypeUnknown Type = iota
	TypeBool
	TypeInt
	TypeUint
	TypeDecimal
	TypeString
	TypeTimestamp
	TypeDate
	TypeJSON
)

type TypedData struct {
	T     Type
	V     interface{}
	Valid bool
}

func NewTypedData(t Type, v interface{}) TypedData {
	return TypedData{
		T:     t,
		V:     v,
		Valid: true,
	}
}

func NewNullTypedData(t Type) TypedData {
	return TypedData{
		T:     t,
		V:     nil,
		Valid: false,
	}
}
