package core

import (
	"github.com/imiskolee/anycdc/pkg/model"
	"time"
)

type ReaderSubscriber interface {
	ReaderEvent(e Event) error
}

type ReaderOption struct {
	Connector  *model.Connector
	Subscriber ReaderSubscriber
	Logger     *FileLogger
	Task       *model.Task
}

type ReaderPosition struct {
	Position    string
	LastEventAt *time.Time
}

type Reader interface {
	Prepare() error
	Start() error
	Stop() error
	LatestPosition() ReaderPosition
	CurrentPosition() ReaderPosition
	Release() error
}
