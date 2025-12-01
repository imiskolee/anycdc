package writer

import "time"

const ()

type SimpleField struct {
	Name string
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
