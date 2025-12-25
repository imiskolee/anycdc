package core

import "github.com/imiskolee/anycdc/pkg/model"

type WriterOption struct {
	Connector *model.Connector
	Logger    *FileLogger
}

type Writer interface {
	Prepare() error
	Execute(e Event) error
	ExecuteBatch(tableName string, records []EventRecord) error
}
