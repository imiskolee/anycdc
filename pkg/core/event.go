package core

import (
	"errors"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/core/types"
)

type EventType int

const (
	EventTypeUnknown EventType = iota
	EventTypeInsert
	EventTypeUpdate
	EventTypeDelete
)

type EventField struct {
	Name  string
	Value types.TypedData
}

type EventRecord struct {
	Columns []EventField
}

func (e *EventRecord) FieldByName(name string) (EventField, error) {
	for _, col := range e.Columns {
		if col.Name == name {
			return col, nil
		}
	}
	return EventField{}, errors.New("field" + name + "not found")
}

func (e *EventRecord) Set(name string, val types.TypedData) {
	for i, col := range e.Columns {
		if col.Name == name {
			e.Columns[i].Value = val
			return
		}
	}
	e.Columns = append(e.Columns, EventField{name, val})
}

func (s *EventRecord) ConvertRecord(sch *schemas.Table) EventRecord {
	fields := make(map[string]types.TypedData)
	for _, field := range s.Columns {
		fields[field.Name] = field.Value
	}
	var newRecord EventRecord
	for _, value := range sch.Columns {
		if _, ok := fields[value.Name]; !ok {
			continue
		}
		newRecord.Set(value.Name, fields[value.Name])
	}
	return newRecord
}

type Field struct {
	Name  string
	Value interface{}
}

type Event struct {
	Type            EventType
	SourceDatabase  string
	SourceTableName string
	Record          EventRecord
	OldRecord       *EventRecord //for update
	SourceSchema    *schemas.Table
}
