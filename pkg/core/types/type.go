package types

type Type uint

const (
	TypeUnknown Type = iota
	TypeNull
	TypeInt
	TypeUint
	TypeFloat64
	TypeDecimal
	TypeString
	TypeBool
	TypeUUID
	TypeBlob
	TypeJSON
	TypeTimestamp
	TypeDate
	TypeTime
)

type Decoder func(v interface{}) (interface{}, error)

type Encoder func(v interface{}) (interface{}, error)

type TypedData struct {
	T Type
	V interface{}
}

func NewTypedData(t Type, v interface{}) TypedData {
	return TypedData{
		T: t,
		V: v,
	}
}

func NewNullData() TypedData {
	return TypedData{
		T: TypeNull,
		V: nil,
	}
}

type Map struct {
	encoders map[Type]Encoder
	decoders map[Type]Decoder
}

func NewMap() *Map {
	return &Map{
		encoders: make(map[Type]Encoder),
		decoders: make(map[Type]Decoder),
	}
}

func (s *Map) RegisterEncoder(t Type, encoder Encoder) {
	s.encoders[t] = encoder
}

func (s *Map) RegisterDecoder(t Type, decoder Decoder) {
	s.decoders[t] = decoder
}

func (s *Map) Encode(t Type, v interface{}) (TypedData, error) {
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
			T: TypeUnknown,
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
