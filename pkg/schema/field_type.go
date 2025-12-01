package schema

type FieldType string

const (
	FieldTypeNumeric   FieldType = "numeric"
	FieldTypeBoolean   FieldType = "boolean"
	FieldTypeDatetime  FieldType = "datetime"
	FieldTypeTimestamp FieldType = "timestamp"
)
