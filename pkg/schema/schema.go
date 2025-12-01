package schema

type Schema struct {
	Schema string
	Table  string
	Fields []Field `json:"fields"`
}
