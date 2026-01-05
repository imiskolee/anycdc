package core

import (
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/model"
)

type WriterOption struct {
	Connector *model.Connector
	Logger    *FileLogger
}

type Writer interface {
	Prepare() error
	Execute(e Event) error
	ExecuteBatch(sourceSchema *schemas.Table, records []EventRecord) error
}
