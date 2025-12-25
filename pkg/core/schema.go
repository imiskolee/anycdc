package core

import (
	"github.com/imiskolee/anycdc/pkg/model"
	"time"
)

type SchemaOption struct {
	Connector *model.Connector
	Logger    *FileLogger
}

type SchemaManager interface {
	Get(dbname string, tableName string) *SimpleTableSchema
	CreateTable(database string, table *SimpleTableSchema) error
}

type SimpleField struct {
	Index            uint
	Type             string
	RawDataType      string
	Nullable         bool
	Name             string
	IsPrimaryKey     bool
	ColumnLength     int
	NumericPrecision int
	NumericScale     int
}

type SimpleTableSchema struct {
	Name       string
	Fields     []SimpleField
	LastSyncAt time.Time
}

func (s *SimpleTableSchema) ConvertRecord(data EventRecord) EventRecord {
	fields := make(map[string]interface{})
	for _, field := range s.Fields {
		fields[field.Name] = field.Name
	}
	var newRecord EventRecord
	for _, value := range data.Columns {
		if _, ok := fields[value.Name]; !ok {
			continue
		}
		newRecord.Set(value.Name, value.Value)
	}
	return newRecord
}

func (s *SimpleTableSchema) GetFieldByIndex(idx uint) (SimpleField, bool) {
	for _, f := range s.Fields {
		if idx == f.Index {
			return f, true
		}
	}
	return SimpleField{}, false
}

func (s *SimpleTableSchema) GetPrimaryKeys() []string {
	var keys []string
	for _, f := range s.Fields {
		if f.IsPrimaryKey {
			keys = append(keys, f.Name)
		}
	}
	return keys
}
