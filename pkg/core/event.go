package core

import (
	"fmt"
)

type EventType int

const (
	EventTyoeUnknown EventType = iota
	EventTypeInsert
	EventTypeUpdate
	EventTypeDelete
)

type Field struct {
	Name  string
	Value interface{}
}

type Event struct {
	Type        EventType
	Database    string
	Schema      string
	Table       string
	PrimaryKeys []Field
	Payload     map[string]interface{}
}

func (e Event) FullTableName() string {
	return fmt.Sprintf("%s.%s", e.Schema, e.Table)
}
