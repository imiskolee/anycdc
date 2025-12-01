package event

type Event struct {
	Type            Type
	Schema          string
	Table           string
	PrimaryKey      string
	PrimaryKeyValue interface{}
	State           string
	Payload         map[string]interface{}
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
