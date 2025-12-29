package core

import (
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/model"
)

type DumperSubscriber interface {
	DumperEvent(sch *schemas.Table, records []EventRecord) error
}

type DumperOption struct {
	Connector  *model.Connector
	Task       *model.Task
	Subscriber DumperSubscriber
	Logger     *FileLogger
	BatchSize  int
}

type Dumper interface {
	Prepare() error
	Stop() error
	StartDumpTable(table *model.TaskTable) error
}
