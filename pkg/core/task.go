package core

import (
	"context"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/model"
	"sync"
	"time"
)

const (
	TaskModeDumper = iota
	TaskModeCDC
)

type taskLogState struct {
	state       *model.TaskTable
	lastEventAt *time.Time
	data        model.TaskTableMetric
}

type metric struct {
	metrics map[string]taskLogState
	mutex   sync.Mutex
	task    *model.Task
}

func (m *metric) add(e *Event) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	met := m.metrics[e.SourceTableName]
	if met.state == nil {
		s, err := model.GetOrCreateTaskTable(m.task.ID, e.SourceTableName)
		if err != nil {
			return
		}
		met.state = s
	}
	switch e.Type {
	case EventTypeInsert:
		met.data.Inserted += 1
		break
	case EventTypeUpdate:
		met.data.Updated += 1
		break
	case EventTypeDelete:
		met.data.Deleted += 1
		break
	default:
	}

	if e.SourceSchema != nil {
		pks := e.SourceSchema.GetPrimaryKeyNames()
		lastSyncRecord := make(map[string]interface{})
		for _, pk := range pks {
			val, err := e.Record.FieldByName(pk)
			if err != nil {
				continue
			}
			lastSyncRecord[pk] = val.Value.V
		}
		met.data.LastSyncedKeys = &lastSyncRecord
	}
	m.metrics[e.SourceTableName] = met

}
func (m *metric) flush(mode string) {
	m.mutex.Lock()
	ms := make(map[string]taskLogState)
	for k, v := range m.metrics {
		ms[k] = v
	}
	for k, _ := range ms {
		d := m.metrics[k]
		d.data = model.TaskTableMetric{}
		m.metrics[k] = d
	}
	m.mutex.Unlock()
	for _, met := range ms {
		if met.state == nil {
			continue
		}
		met.data.Mode = mode
		_ = met.state.Flush(met.data)
	}
}

type State struct {
	Reader   *model.Connector
	Writer   *model.Connector
	Task     *model.Task
	TaskLogs map[string]model.TaskTable
}

type Task struct {
	id                string
	state             State
	tables            []string
	dumper            Dumper
	reader            Reader
	writer            Writer
	logger            *FileLogger
	ctx               context.Context
	dumpStartPosition string
	metric            metric
	dumperRunning     bool
	cdcRunning        bool
	dumperWG          sync.WaitGroup
}

func NewTask(id string) *Task {
	return &Task{
		ctx:    context.Background(),
		id:     id,
		logger: NewFileLog(fmt.Sprintf("tasks/%s.log", id), LevelInfo),
		metric: metric{
			metrics: make(map[string]taskLogState),
			task:    &model.Task{Base: model.Base{ID: id}},
		},
	}
}

func (s *Task) Prepare() error {
	task, err := model.GetTaskByID(s.id)
	if err != nil {
		return s.logger.Errorf("can not load task %s, %s", s.id, err)
	}
	readerConnector, err := model.GetConnectorByID(task.Reader)
	if err != nil {
		return s.logger.Errorf("can not load reader connector %s, %s", task.Reader, err)
	}
	writerConnector, err := model.GetConnectorByID(task.Writer)
	if err != nil {
		return s.logger.Errorf("can not load writer connector %s, %s", task.Writer, err)
	}
	s.state.Task = task
	s.state.Reader = readerConnector
	s.state.Writer = writerConnector
	return nil
}

func (s *Task) Start() error {
	shouldDumper := false
	if s.state.Task.DumperEnabled {
		for _, table := range s.state.Task.GetTables() {
			t, err := model.GetOrCreateTaskTable(s.state.Task.ID, table)
			if err != nil {
				continue
			}
			if t.DumperState == model.DumperStateInitialed || t.DumperState == model.DumperStatusRunning {
				shouldDumper = true
				break
			}
		}
	}
	if shouldDumper {
		if err := s.startDumper(); err != nil {
			return err
		}
	}
	if s.state.Task.CDCEnabled && s.state.Task.CDCStatus != model.CDCStatusStopped {
		if err := s.startCDC(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Task) Stop() error {
	if s.dumper != nil {
		_ = s.stopDumper()
	}
	if s.reader != nil {
		_ = s.stopCDC()
	}
	return nil
}

func (s *Task) startCDC() error {
	s.cdcRunning = true
	_ = s.state.Task.UpdateCDCStatus(model.CDCStatusRunning)
	success := false
	defer (func() {
		_ = s.Save()
		s.cdcRunning = false
		if success {
			_ = s.state.Task.UpdateCDCStatus(model.CDCStatusStopped)
		} else {
			_ = s.state.Task.UpdateCDCStatus(model.CDCStatusFailed)
		}
	})()
	var readerPlugin Plugin
	var writerPlugin Plugin
	var ok bool
	{
		readerPlugin, ok = GetPlugin(s.state.Reader.Type)
		if !ok {
			return s.logger.Errorf("can not find plugin:%s", s.state.Reader.Type)
		}
		if readerPlugin.ReaderFactory == nil {
			return s.logger.Errorf("plugin %s have not supports reader protocol", s.state.Reader.Type)
		}
		s.reader = readerPlugin.ReaderFactory(s.ctx, &ReaderOption{
			Connector:  s.state.Reader,
			Subscriber: s,
			Logger:     s.logger,
			Task:       s.state.Task,
		})
	}
	{
		writerPlugin, ok = GetPlugin(s.state.Writer.Type)
		if !ok {
			return s.logger.Errorf("can not find plugin:%s", s.state.Writer.Type)
		}
		if writerPlugin.WriterFactory == nil {
			return s.logger.Errorf("plugin %s have not supports writer protocol", s.state.Writer.Type)
		}
		s.writer = writerPlugin.WriterFactory(s.ctx, &WriterOption{
			Connector: s.state.Writer,
			Logger:    s.logger,
		})
	}
	if err := s.reader.Prepare(); err != nil {
		return s.logger.Errorf("can not prepare reader: %s", err)
	}
	if err := s.writer.Prepare(); err != nil {
		return s.logger.Errorf("can not prepare writer: %s", err)
	}
	if err := s.reader.Start(); err != nil {
		return err
	}
	success = true
	return nil
}

func (s *Task) stopCDC() error {
	_ = s.Save()
	err := s.reader.Stop()
	return err
}

func (s *Task) startDumper() error {
	s.logger.Info("starting dumper")
	defer (func() {
		_ = s.Save()
		s.dumperRunning = false
	})()
	var readerPlugin Plugin
	var writerPlugin Plugin
	var ok bool
	{
		readerPlugin, ok = GetPlugin(s.state.Reader.Type)
		if !ok {
			return s.logger.Errorf("can not find plugin %s", s.state.Reader.Type)
		}
		if readerPlugin.DumperFactory == nil {
			return s.logger.Errorf("plugin %s have not supports dumper protocol", s.state.Reader.Type)
		}
		dumper := readerPlugin.DumperFactory(s.ctx, &DumperOption{
			Task:       s.state.Task,
			Connector:  s.state.Reader,
			Subscriber: s,
			Logger:     s.logger,
			BatchSize:  1000,
		})
		if readerPlugin.ReaderFactory != nil {
			reader := readerPlugin.ReaderFactory(s.ctx, &ReaderOption{
				Connector:  s.state.Reader,
				Subscriber: s,
				Logger:     s.logger,
				Task:       s.state.Task,
			})
			if err := reader.Prepare(); err != nil {
				return s.logger.Errorf("can not prepare reader: %s", err)
			}
			po := reader.LatestPosition()
			s.state.Task.LastCDCPosition = po.Position
			s.logger.Info("cdc reader will read from position %s", s.state.Task.LastCDCPosition)
			if err := s.state.Task.PartialUpdates(map[string]interface{}{
				"last_cdc_position": s.state.Task.LastCDCPosition,
			}); err != nil {
				s.logger.Error("can not save latest cdc position")
			}
			_ = reader.Stop()
		}
		s.dumper = dumper
	}
	{
		writerPlugin, ok = GetPlugin(s.state.Writer.Type)
		if !ok {
			return s.logger.Errorf("can not find plugin %s", s.state.Writer.Type)
		}
		if writerPlugin.WriterFactory == nil {
			return s.logger.Errorf("plugin %s have not supports writer protocol", s.state.Writer.Type)
		}
		writer := writerPlugin.WriterFactory(s.ctx, &WriterOption{
			Connector: s.state.Writer,
			Logger:    s.logger,
		})
		s.writer = writer
	}

	if s.state.Task.MigrateEnabled {
		if err := s.migrateTables(&readerPlugin, &writerPlugin); err != nil {
			return err
		}
	}
	s.dumperRunning = true
	if err := s.dumper.Prepare(); err != nil {
		return s.logger.Errorf("dumper prepare fail, %s", err)
	}

	if err := s.writer.Prepare(); err != nil {
		return s.logger.Errorf("writer prepare fail, %s", err)
	}
	if err := s.startDumperTask(); err != nil {
		return s.logger.Errorf("dumper start fail, %s", err)
	}
	return nil
}

func (s *Task) stopDumper() error {
	if s.dumper == nil {
		return nil
	}
	err := s.dumper.Stop()
	s.dumper = nil
	return err
}

func (s *Task) DumperEvent(sch *schemas.Table, records []EventRecord) error {
	if len(records) < 1 {
		return nil
	}
	if err := s.writer.ExecuteBatch(sch.Name, records); err != nil {
		return err
	}
	for _, record := range records {
		var e Event
		e.Type = EventTypeInsert
		e.SourceDatabase = s.state.Reader.Database
		e.SourceTableName = sch.Name
		e.Record = record
		e.SourceSchema = sch
		s.metric.add(&e)
	}
	return nil
}

func (s *Task) ReaderEvent(e Event) error {
	s.metric.add(&e)
	return s.writer.Execute(e)
}

func (s *Task) Save() error {
	if s.dumperRunning {
		s.summary()
		s.metric.flush(model.TaskModeDumper)
		return nil
	}
	if s.cdcRunning {
		currentPosition := s.reader.CurrentPosition()
		if currentPosition.Position != s.state.Task.LastCDCPosition {
			s.summary()
			s.state.Task.LastCDCPosition = currentPosition.Position
			updates := map[string]interface{}{
				"last_cdc_position": s.state.Task.LastCDCPosition,
			}
			if currentPosition.LastEventAt != nil {
				updates["last_cdc_at"] = currentPosition.LastEventAt
			}
			if err := s.state.Task.PartialUpdates(updates); err != nil {
				s.logger.Error("can not save latest cdc position")
			}
		}
		s.metric.flush(model.TaskModeCDC)
	}

	return nil
}

func (s *Task) migrateTables(readerPlugin *Plugin, writerPlugin *Plugin) error {
	s.logger.Info("starting migrating tables")
	var readerSchManager SchemaManager
	var writerSchManager SchemaManager
	if readerPlugin.SchemaFactory == nil {
		return s.logger.Errorf("can not find reader schema factory for %s", readerPlugin.Name)
	}
	if writerPlugin.SchemaFactory == nil {
		return s.logger.Errorf("can not find writer schema factory for %s", writerPlugin.Name)
	}

	readerSchManager = readerPlugin.SchemaFactory(context.Background(), &SchemaOption{
		Connector: s.state.Reader,
		Logger:    s.logger,
	})
	writerSchManager = writerPlugin.SchemaFactory(context.Background(), &SchemaOption{
		Connector: s.state.Writer,
		Logger:    s.logger,
	})

	for _, table := range s.state.Task.GetTables() {
		if err := s.migrateTable(readerSchManager, writerSchManager, table); err != nil {
			return err
		}
	}
	return nil
}

func (s *Task) migrateTable(readerSchManager SchemaManager, writerSchManager SchemaManager, tableName string) error {
	readerTableSchema := readerSchManager.Get(s.state.Reader.Database, tableName)
	writerTableSchema := writerSchManager.Get(s.state.Writer.Database, tableName)
	if writerTableSchema != nil && len(writerTableSchema.Columns) > 0 {
		s.logger.Info("skip migrate table %s, because of already exists on writer connection", tableName)
		return nil
	}
	if len(readerTableSchema.GetPrimaryKeys()) < 1 {
		return s.logger.Errorf("can not find primary key for table %s", tableName)
	}
	if err := writerSchManager.CreateTable(readerTableSchema); err != nil {
		return s.logger.Errorf("can not migrate table %s, %s", tableName, err)
	}
	s.logger.Info("success migrate table %s", tableName)
	return nil
}

func (s *Task) startDumperTask() error {
	tables := s.state.Task.GetTables()
	s.logger.Info("start dumper task on task %s, tables=%+v", s.state.Task.Name, tables)
	var globalErr error
	for _, table := range tables {
		s.dumperWG.Add(1)
		go (func() {
			defer s.dumperWG.Done()
			if err := s.startDumpTable(table); err != nil {
				globalErr = err
				s.logger.Error("failed to run dump task on table %s,%s", table, err)
			}
		})()
	}
	s.dumperWG.Wait()
	return globalErr
}

func (s *Task) startDumpTable(tableName string) error {
	var err error
	if s.dumper == nil {
		return nil
	}
	s.logger.Info("starting dump table %s", tableName)
	taskTable, err := model.GetOrCreateTaskTable(s.state.Task.ID, tableName)
	if err != nil {
		return err
	}
	if taskTable.DumperState == model.DumperStateCompleted {
		s.logger.Info("skipped table %s, it's already completed", tableName)
		return nil
	}
	if taskTable.DumperState == model.DumperStateFailed {
		s.logger.Info("skipped table %s, please manual re-trigger dumper", tableName)
		return nil
	}
	taskTable.UpdateDumperState(model.DumperStateRunning)
	defer (func() {
		if err != nil {
			s.logger.Info("table %s dumper failed, %s", tableName, err.Error())
			taskTable.UpdateDumperState(model.DumperStateFailed)
		} else {
			s.logger.Info("table %s dumper completed", tableName)
			taskTable.UpdateDumperState(model.DumperStateCompleted)
		}
	})()
	if err = s.dumper.StartDumpTable(taskTable); err != nil {
		return err
	}
	return nil
}

func (s *Task) summary() {
	s.logger.Info("Task Summary Report:")
	s.logger.Info("  CDC Position:%s", s.state.Task.LastCDCPosition)
	for _, m := range s.metric.metrics {
		s.logger.Info("  table %s I=%d, U=%d, D=%d", m.state.Table, m.data.Inserted, m.data.Updated, m.data.Deleted)
	}
}

func (s *Task) Release() error {
	if s.reader != nil {
		return s.reader.Release()
	}
	return nil
}
