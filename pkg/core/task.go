package core

import (
	"context"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/panjf2000/ants/v2"
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
	metrics sync.Map
	mutex   sync.Mutex
	task    *model.Task
}

func (m *metric) add(e *Event) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	var tl taskLogState
	met, ok := m.metrics.Load(e.SourceSchema.Name)
	if !ok {
		s, err := model.GetTaskTableByName(m.task.ID, e.SourceSchema.Name)
		if err != nil {
			return
		}
		tl.state = s
	} else {
		tl = met.(taskLogState)
	}
	switch e.Type {
	case EventTypeInsert:
		tl.data.Inserted += 1
		break
	case EventTypeUpdate:
		tl.data.Updated += 1
		break
	case EventTypeDelete:
		tl.data.Deleted += 1
		break
	default:
	}

	if e.SourceSchema.Name != "" {
		pks := e.SourceSchema.GetPrimaryKeyNames()
		lastSyncRecord := make(map[string]interface{})
		for _, pk := range pks {
			val, err := e.Record.FieldByName(pk)
			if err != nil {
				continue
			}
			lastSyncRecord[pk] = val.Value.V
		}
		tl.data.LastSyncedKeys = &lastSyncRecord
	}
	m.metrics.Store(e.SourceSchema.Name, tl)
}
func (m *metric) flush(mode string) {
	m.mutex.Lock()
	ms := make(map[string]taskLogState)

	m.metrics.Range(func(key, value interface{}) bool {
		ms[key.(string)] = value.(taskLogState)
		return true
	})
	for k, _ := range ms {
		d := ms[k]
		d.data = model.TaskTableMetric{}
		m.metrics.Store(k, d)
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
	tables            []model.TableDefine
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
	threadPool        *ants.Pool
	tableErrors       sync.Map
	lastSaveAt        time.Time
}

func NewTask(id string) *Task {
	level := LevelInfo
	task, _ := model.GetTaskByID(id)
	if task != nil {
		if task.DebugEnabled {
			level = LevelDebug
		}
	}
	return &Task{
		ctx:    context.Background(),
		id:     id,
		logger: NewFileLog(fmt.Sprintf("tasks/%s.log", id), level),
		metric: metric{
			task: &model.Task{Base: model.Base{ID: id}},
		},
		lastSaveAt: time.Now(),
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
	task.Preload()
	s.state.Task = task
	s.state.Reader = readerConnector
	s.state.Writer = writerConnector
	s.tables = task.GetTables()
	return nil
}

func (s *Task) Start() error {
	shouldDumper := false
	if s.state.Task.DumperEnabled {
		for _, table := range s.tables {
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
	s.lastSaveAt = time.Now()
	if s.state.Task.CDCEnabled {
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
	if s.cdcRunning == true {
		if s.reader != nil {
			_ = s.stopCDC()
		}
	}
	return nil
}

func (s *Task) startCDC() error {
	maxNumber := s.state.Task.ThreadNumber
	if maxNumber == 0 {
		maxNumber = 5
	}
	p, _ := ants.NewPool(maxNumber)
	s.threadPool = p
	defer (func() {
		s.threadPool.Release()
		s.threadPool = nil
	})()
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
	_ = s.state.Task.UpdateCDCStatus(model.CDCStatusStopped)
	return err
}

func (s *Task) startDumper() error {
	s.logger.Info("starting dumper")
	maxNumber := s.state.Task.ThreadNumber
	if maxNumber == 0 {
		maxNumber = 5
	}
	p, _ := ants.NewPool(maxNumber)
	s.threadPool = p
	defer (func() {
		s.threadPool.Release()
		s.threadPool = nil
	})()
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
	err, ok := s.tableErrors.Load(sch.Name)
	if ok && err != nil {
		return err.(error)
	}
	return s.threadPool.Submit(s.runDumperEvent(sch, records))
}

func (s *Task) runDumperEvent(sch *schemas.Table, records []EventRecord) func() {
	return func() {
		var events []Event
		for _, r := range records {
			events = append(events, Event{
				Type:                 EventTypeInsert,
				Record:               r,
				SourceSchema:         *sch,
				DestinationTableName: s.getDestinationTable(sch.Name),
			})
		}
		if err := s.writer.ExecuteBatch(sch, events); err != nil {
			s.tableErrors.Store(sch.Name, err)
			return
		}
		s.tableErrors.Delete(sch.Name)
		for _, record := range records {
			var e Event
			e.Type = EventTypeInsert
			e.DestinationTableName = s.getDestinationTable(sch.Name)
			e.Record = record
			e.SourceSchema = *sch
			s.metric.add(&e)
		}
	}
}

func (s *Task) ReaderEvent(e Event) error {
	err, ok := s.tableErrors.Load(e.SourceSchema.Name)
	if ok && err != nil {
		return err.(error)
	}
	return s.threadPool.Submit(s.runTask(e))
}

func (s *Task) runTask(e Event) func() {
	return func() {
		s.metric.add(&e)
		e.DestinationTableName = s.getDestinationTable(e.SourceSchema.Name)
		err := s.writer.Execute(e)
		if err != nil {
			s.tableErrors.Store(e.SourceSchema.Name, err)
		} else {
			s.tableErrors.Delete(e.SourceSchema.Name)
		}
	}
}

func (s *Task) Save() error {
	task, err := model.GetTaskByID(s.state.Task.ID)
	if err != nil {
		return err
	}
	if task.CDCStatus != model.CDCStatusRunning {
		return nil
	}
	now := time.Now()
	if task.CDCDelayTime > 0 {
		if now.Sub(s.lastSaveAt) < time.Duration(task.CDCDelayTime)*time.Minute {
			return nil
		}
	}
	s.lastSaveAt = now
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

	for _, table := range s.tables {
		if err := s.migrateTable(readerSchManager, writerSchManager, table); err != nil {
			return err
		}
	}
	return nil
}

func (s *Task) migrateTable(readerSchManager SchemaManager, writerSchManager SchemaManager, table model.TableDefine) error {
	readerTableSchema := readerSchManager.Get(s.state.Reader.Database, table.SourceTable)
	writerTableSchema := writerSchManager.Get(s.state.Writer.Database, table.DestinationTable)
	if writerTableSchema != nil && len(writerTableSchema.Columns) > 0 {
		s.logger.Info("skip migrate table %s, because of already exists on writer connection", table.DestinationTable)
		return nil
	}
	if len(readerTableSchema.GetPrimaryKeys()) < 1 {
		return s.logger.Errorf("can not find primary key for table %s", table.SourceTable)
	}
	readerTableSchema.Name = table.DestinationTable
	if err := writerSchManager.CreateTable(readerTableSchema); err != nil {
		return s.logger.Errorf("can not migrate table %s, %s", table.SourceTable, err)
	}
	s.logger.Info("success migrate table %s", table.SourceTable)
	return nil
}

func (s *Task) startDumperTask() error {
	tables := s.tables
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
	s.threadPool.Waiting()

	return globalErr
}

func (s *Task) startDumpTable(table model.TableDefine) error {
	var err error
	if s.dumper == nil {
		return nil
	}
	s.logger.Info("starting dump table %s", table.SourceTable)
	taskTable, err := model.GetOrCreateTaskTable(s.state.Task.ID, table)
	if err != nil {
		return err
	}
	if taskTable.DumperState == model.DumperStateCompleted {
		s.logger.Info("skipped table %s, it's already completed", table.SourceTable)
		return nil
	}
	if taskTable.DumperState == model.DumperStateFailed {
		s.logger.Info("skipped table %s, please manual re-trigger dumper", table.SourceTable)
		return nil
	}
	taskTable.UpdateDumperState(model.DumperStateRunning)
	defer (func() {
		if err != nil {
			s.logger.Info("table %s dumper failed, %s", table.SourceTable, err.Error())
			taskTable.UpdateDumperState(model.DumperStateFailed)
		} else {
			s.logger.Info("table %s dumper completed", table.SourceTable)
			taskTable.UpdateDumperState(model.DumperStateCompleted)
		}
	})()
	if err = s.dumper.StartDumpTable(taskTable); err != nil {
		return err
	}
	for {
		if s.threadPool.Running() > 0 {
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}
	return nil
}

func (s *Task) summary() {
	s.logger.Info("Task Summary Report:")
	s.logger.Info("  CDC Position:%s", s.state.Task.LastCDCPosition)
	s.metric.metrics.Range(func(k, v interface{}) bool {
		m := v.(taskLogState)
		s.logger.Info("  table %s I=%d, U=%d, D=%d", m.state.Table, m.data.Inserted, m.data.Updated, m.data.Deleted)
		return true
	})
}

func (s *Task) Release() error {
	if s.reader != nil {
		return s.reader.Release()
	}
	return nil
}

func (s *Task) getDestinationTable(sourceTableName string) string {
	for _, t := range s.tables {
		if t.SourceTable == sourceTableName {
			return t.DestinationTable
		}
	}
	return sourceTableName
}
