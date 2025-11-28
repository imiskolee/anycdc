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
