package task

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/event"
	"bindolabs/anycdc/pkg/reader"
	"bindolabs/anycdc/pkg/state"
	"bindolabs/anycdc/pkg/writer"
)

type Task struct {
	conf    config.Task
	reader  reader.Reader
	writers []writer.Writer
	buffer  []event.Event
}

func NewTask(task config.Task) *Task {
	return &Task{
		conf: task,
	}
}

func (t *Task) Prepare() error {
	r := reader.NewReader(t.conf.Reader, &reader.ReaderOptions{
		Subscriber:  t,
		StateLoader: state.NewState(t.conf.Name),
	})
	_ = r.Prepare()
	t.reader = r
	t.writers = make([]writer.Writer, 0)
	for _, w := range t.conf.Writers {
		c, _ := config.GetConnector(w.Connector)
		wr := writer.NewWriter(c)
		err := wr.Prepare()
		if err != nil {
			panic(err)
		}
		t.writers = append(t.writers, wr)
	}
	return nil
}

func (t *Task) Start() error {
	return t.reader.Start()
}

func (t *Task) Consume(event event.Event) error {
	for _, w := range t.writers {
		_ = w.Execute(event)
	}
	return nil
}
