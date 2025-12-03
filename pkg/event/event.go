package event

import "fmt"

type Event struct {
	Type            Type
	Schema          string
	Table           string
	PrimaryKey      string
	PrimaryKeyValue interface{}
	State           string
	Payload         map[string]interface{}
}

func (e Event) FullTableName() string {
	return fmt.Sprintf("%s.%s", e.Schema, e.Table)
}

func (e Event) Copy() Event {
	return Event{
		Type:            e.Type,
		Schema:          e.Schema,
		Table:           e.Table,
		PrimaryKey:      e.PrimaryKey,
		PrimaryKeyValue: e.PrimaryKeyValue,
		State:           e.State,
		Payload:         e.Payload,
	}
}
