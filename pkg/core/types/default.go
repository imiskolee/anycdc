package types

import "github.com/imiskolee/anycdc/pkg/core/schemas"

func NewDefaultTypeMap() *Map {
	m := NewMap()
	m.RegisterEncoder(schemas.TypeInt, DefaultIntEncoder)
	m.RegisterEncoder(schemas.TypeUint, DefaultUintEncoder)
	m.RegisterEncoder(schemas.TypeDecimal, DefaultFloatEncoder)
	m.RegisterEncoder(schemas.TypeBool, DefaultBoolEncoder)
	m.RegisterEncoder(schemas.TypeJSON, DefaultJSONEncoder)
	m.RegisterEncoder(schemas.TypeString, DefaultStringEncoder)
	m.RegisterEncoder(schemas.TypeUUID, DefaultUUIDEncoder)
	m.RegisterEncoder(schemas.TypeTime, DefaultTimeEncoder)
	m.RegisterEncoder(schemas.TypeDate, DefaultDateEncoder)
	m.RegisterEncoder(schemas.TypeTimestamp, DefaultTimestampEncoder)
	m.RegisterEncoder(schemas.TypeBlob, DefaultBlobEncoder)
	m.RegisterDecoder(schemas.TypeJSON, DefaultJSONDecoder)
	return m
}
