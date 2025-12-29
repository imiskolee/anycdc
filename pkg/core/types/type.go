package types

import "github.com/imiskolee/anycdc/pkg/core/schemas"

type Decoder func(v interface{}) (interface{}, error)

type Encoder func(v interface{}) (interface{}, error)

type TypedData struct {
	T schemas.Type
	V interface{}
}

func NewTypedData(t schemas.Type, v interface{}) TypedData {
	return TypedData{
		T: t,
		V: v,
	}
}

func NewNullData() TypedData {
	return TypedData{
		T: schemas.TypeNull,
		V: nil,
	}
}

type Map struct {
	encoders map[schemas.Type]Encoder
	decoders map[schemas.Type]Decoder
}

func NewMap() *Map {
	return &Map{
		encoders: make(map[schemas.Type]Encoder),
		decoders: make(map[schemas.Type]Decoder),
	}
}

func (s *Map) RegisterEncoder(t schemas.Type, encoder Encoder) {
	s.encoders[t] = encoder
}

func (s *Map) RegisterDecoder(t schemas.Type, decoder Decoder) {
	s.decoders[t] = decoder
}

func (s *Map) Encode(t schemas.Type, v interface{}) (TypedData, error) {
	switch val := v.(type) {
	case TypedData:
		return val, nil
	case *TypedData:
		return *val, nil
	}
	if v == nil {
		return NewNullData(), nil
	}
	e, ok := s.encoders[t]
	if !ok {
		return TypedData{
			T: t,
			V: v,
		}, nil
	}
	val, err := e(v)
	if err != nil {
		return TypedData{
			T: schemas.TypeUnknown,
		}, err
	}
	return TypedData{
		T: t,
		V: val,
	}, nil
}

func (s *Map) Decode(v TypedData) (interface{}, error) {
	e, ok := s.decoders[v.T]
	if !ok {
		return v.V, nil
	}
	if v.V == nil {
		return nil, nil
	}
	return e(v.V)
}
