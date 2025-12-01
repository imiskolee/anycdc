package schema

type FieldType string

const (
	FieldTypeInt       FieldType = "int"
	FieldTypeDecimal   FieldType = "decimal"
	FieldTypeBoolean   FieldType = "boolean"
	FieldTypeDatetime  FieldType = "datetime"
	FieldTypeTimestamp FieldType = "timestamp"
)
