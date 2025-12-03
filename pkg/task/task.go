package task

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/event"
	"bindolabs/anycdc/pkg/reader"
	"bindolabs/anycdc/pkg/state"
	"bindolabs/anycdc/pkg/writer"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Metric struct {
	LastEventAt time.Time
	SyncedEvent map[string]map[event.Type]uint64
}

func (m *Metric) NewEvent(schema string, t event.Type) {
	if _, ok := m.SyncedEvent[schema]; ok {
		m.SyncedEvent[schema][t] += 1
	} else {
		m.SyncedEvent[schema] = map[event.Type]uint64{
			t: 1,
		}
	}
}

type Task struct {
	conf    config.Task
	reader  reader.Reader
	writers []writer.Writer
	buffer  []event.Event
	Metric  Metric
}

func NewTask(task config.Task) *Task {
	return &Task{
		conf: task,
		Metric: Metric{
			SyncedEvent: map[string]map[event.Type]uint64{},
		},
	}
}

func (t *Task) Prepare() error {
	r := reader.NewReader(t.conf.Reader, &reader.ReaderOptions{
		Subscriber:  t,
		StateLoader: state.NewState(t.conf.Name),
	})
	if err := r.Prepare(); err != nil {
		return err
	}
	t.reader = r
	t.writers = make([]writer.Writer, 0)
	for _, w := range t.conf.Writers {
		wr := writer.NewWriter(w)
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
	return t.consume(&event)
}

func (t *Task) consume(event *event.Event) error {
	wg := &sync.WaitGroup{}
	var hasError atomic.Bool
	for _, w := range t.writers {
		wg.Add(1)
		go (func() {
			defer wg.Done()
			var err error
			for i := 0; i < 3; i++ {
				err = w.Execute(*event)
				if err != nil {
					log.Println("Failed to execute event:", err)
					continue
				}
			}
			if err != nil {
				hasError.Store(true)
			}
		})()
	}
	wg.Wait()
	if hasError.Load() {
		return errors.New("Failed to execute event")
	}
	t.Metric.NewEvent(event.FullTableName(), event.Type)
	return nil
}

func (t *Task) SaveState() error {
	return t.reader.Save()
}
