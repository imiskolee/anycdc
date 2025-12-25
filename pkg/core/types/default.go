package types

func NewDefaultTypeMap() *Map {
	m := NewMap()
	m.RegisterEncoder(TypeInt, DefaultIntEncoder)
	m.RegisterEncoder(TypeUint, DefaultUintEncoder)
	m.RegisterEncoder(TypeFloat64, DefaultFloatEncoder)
	m.RegisterEncoder(TypeDecimal, DefaultFloatEncoder)
	m.RegisterEncoder(TypeBool, DefaultBoolEncoder)
	m.RegisterEncoder(TypeJSON, DefaultJSONEncoder)
	m.RegisterEncoder(TypeString, DefaultStringEncoder)
	m.RegisterEncoder(TypeUUID, DefaultUUIDEncoder)
	m.RegisterEncoder(TypeTime, DefaultTimeEncoder)
	m.RegisterEncoder(TypeDate, DefaultDateEncoder)
	m.RegisterEncoder(TypeTimestamp, DefaultTimestampEncoder)
	m.RegisterEncoder(TypeBlob, DefaultBlobEncoder)

	m.RegisterDecoder(TypeJSON, DefaultJSONDecoder)
	return m
}
