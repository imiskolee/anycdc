package core

import "time"

type SchemaOption struct {
	Connector string
	Logger    *FileLogger
}

type SchemaManager interface {
	Get(dbname string, tableName string) *SimpleTableSchema
}

type SimpleField struct {
	Index        uint
	Type         string
	Name         string
	IsPrimaryKey bool
}

type SimpleTableSchema struct {
	Name       string
	Fields     []SimpleField
	LastSyncAt time.Time
}

func (s *SimpleTableSchema) ConvertRecord(data map[string]interface{}) map[string]interface{} {
	fields := make(map[string]interface{})
	for _, field := range s.Fields {
		fields[field.Name] = field.Name
	}
	newRecord := make(map[string]interface{})
	for key, value := range data {
		if _, ok := fields[key]; !ok {
			continue
		}
		newRecord[key] = value
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
