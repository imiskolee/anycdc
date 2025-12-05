package types

type TypedData interface {
	Marshal() interface{}
}

type Encoder func(v interface{}) (TypedData, error)

var _factory = map[string]Encoder{}
