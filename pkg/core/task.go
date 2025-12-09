package core

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/model"
	"strings"
	"time"
)

type Task struct {
	id               string
	task             *model.Task
	Reader           Reader
	Writers          []Writer
	logger           *FileLogger
	ctx              context.Context
	metric           model.Metric
	lastMetricSyncAt time.Time
}

func NewTask(id string) *Task {
	return &Task{
		ctx:    context.Background(),
		id:     id,
		logger: NewFileLog(fmt.Sprintf("tasks/%s.log", id)),
	}
}

func (s *Task) Prepare() error {
	t, err := model.GetTaskByID(s.id)
	if err != nil {
		return s.logger.Errorf("can not get task by id %s,%s", s.id, err)
	}
	s.task = t
	readerConnector, err := model.GetConnectorByID(t.Reader)
	if err != nil {
		return s.logger.Errorf("can not get reader connector by id %s,%s", s.id, err)
	}
	fmt.Println("extra", t.Extras)
	extra := make(model.Extra)
	if t.Extras != "" {
		if err := json.Unmarshal([]byte(t.Extras), &extra); err != nil {
			return s.logger.Errorf("can not parse extras %s,%s", t.Extras, err)
		}
	}
	readerFactory := Registries.Reader.Get(readerConnector.Type)
	s.Reader = readerFactory(s.ctx, &ReaderOption{
		Connector:       t.Reader,
		Tables:          strings.Split(t.Tables, ","),
		Logger:          s.logger,
		Subscriber:      s,
		InitialPosition: t.LastPosition,
		Extra:           extra,
	})
	if err := s.Reader.Prepare(); err != nil {
		return s.logger.Errorf("can not prepare reader %s,%s", s.id, err)
	}
	var writers []string
	if err := json.Unmarshal([]byte(t.Writers), &writers); err != nil {
		return s.logger.Errorf("can not unmarshal writers %s,%s", s.id, err)
	}
	for _, writer := range writers {
		writerConnector, err := model.GetConnectorByID(writer)
		if err != nil {
			return s.logger.Errorf("can not get reader connector by id %s,%s", s.id, err)
		}
		writerFactory := Registries.Writer.Get(writerConnector.Type)
		w := writerFactory(s.ctx, &WriterOption{
			Connector: writer,
			Logger:    s.logger,
		})
		if err := w.Prepare(); err != nil {
			return s.logger.Errorf("can not prepare writer %s,%s", writer, err)
		}
		s.Writers = append(s.Writers, w)
	}
	return nil
}

func (s *Task) Start() error {
	return s.Reader.Start()
}

func (s *Task) Stop() error {
	return s.Reader.Stop()
}

func (s *Task) Save() {
	s.metric.LastSyncAt = time.Now()
	s.metric.LastSyncPosition = s.Reader.Position()
	s.logger.Info("save task state %s on %s", s.metric.LastSyncPosition, s.metric.LastSyncAt)
	s.save()
}

func (s *Task) Event(e Event) error {
	for _, w := range s.Writers {
		if err := w.Execute(e); err != nil {
			return s.logger.Errorf("can not execute writer %s,%s", w, err)
		}
	}
	s.metric.LastSyncAt = time.Now()
	s.metric.LastSyncPosition = s.Reader.Position()
	s.updateMetric(e.Type)
	return nil
}

func (s *Task) updateMetric(typ EventType) {
	switch typ {
	case EventTypeInsert:
		s.metric.Inserted++
		break
	case EventTypeUpdate:
		s.metric.Updated++
		break
	case EventTypeDelete:
		s.metric.Deleted++
	}
	now := time.Now()
	if now.Sub(s.lastMetricSyncAt) > 60*time.Second {
		if err := s.save(); err == nil {
			s.lastMetricSyncAt = now
		}
	}

}

func (s *Task) save() error {
	s.lastMetricSyncAt = time.Now()
	metric := s.metric
	err := s.task.UpdateMetric(metric)
	if err != nil {
		s.logger.Error("failed update task metric,%s", err)
		return err
	}
	s.metric.Inserted = 0
	s.metric.Updated = 0
	s.metric.Deleted = 0
	return nil
}
