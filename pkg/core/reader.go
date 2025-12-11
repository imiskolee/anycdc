package core

import (
	"github.com/imiskolee/anycdc/pkg/model"
	"time"
)

type ReaderSubscriber interface {
	Event(e Event) error
}

type ReaderOption struct {
	Connector       string
	Tables          []string
	Subscriber      ReaderSubscriber
	Logger          *FileLogger
	InitialPosition string
	Extra           model.Extra
}

type Reader interface {
	Prepare() error
	Start() error
	Stop() error
	Position() string
	LastEventAt() time.Time
}
