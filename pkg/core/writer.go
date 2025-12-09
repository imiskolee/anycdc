package core

type WriterOption struct {
	Connector string
	Logger    *FileLogger
}

type Writer interface {
	Prepare() error
	Execute(e Event) error
}
