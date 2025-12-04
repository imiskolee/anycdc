package elasticsearch

import "github.com/imiskolee/anycdc/pkg/event"

type Writer struct {
}

func (s *Writer) Prepare() error {
	return nil
}

func (s *Writer) Consume(e event.Event) error {
	return nil
}

func (s *Writer) Start() error {
	return nil
}

func (s *Writer) Stop() error {
	return nil
}
