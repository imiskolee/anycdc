package mysql

import (
	"context"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"gorm.io/gorm"
	"sync"
	"time"
)

type writer struct {
	opt           *core.WriterOption
	conn          *gorm.DB
	schemaManager core.SchemaManager
	Pipeline      *core.Pipeline
	mutex         sync.Mutex
}

func NewWriter(ctx context.Context, opt interface{}) core.Writer {
	o := opt.(*core.WriterOption)
	return &writer{
		opt:      o,
		Pipeline: core.NewPipeline(),
		schemaManager: core.NewCachedSchemaManager(NewSchema(context.Background(), &core.SchemaOption{
			Connector: o.Connector,
			Logger:    o.Logger,
		})),
	}
}

func (w *writer) Prepare() error {
	db, err := Connect(w.opt.Connector)
	if err != nil {
		w.opt.Logger.Error("can not prepare connector:%s,%s", w.opt.Connector, err)
		return err
	}
	db.Logger = common_sql.NewLogger(w.opt.Logger)
	w.conn = db
	return nil
}

func (w *writer) Execute(e core.Event) error {
	sch := w.schemaManager.Get(w.opt.Connector.Database, e.DestinationTableName)
	if len(sch.Columns) < 1 {
		w.opt.Logger.Debug("Skipped event, table %s do not exists on the connector", e.DestinationTableName)
		return nil
	}

	if e.Type == core.EventTypeUpdate {
		e.Type = core.EventTypeInsert
	}

	if w.opt.Connector.Type == model.ConnectorTypeStarRocks {
		if e.Type == core.EventTypeDelete {
			return nil
		}
		w.appendBatch(e)
		if time.Now().Sub(w.Pipeline.CreatedAt) > 60*time.Second || w.Pipeline.Count > 5000 {
			return w.processBatch()
		}
		return nil
	}
	e.Record = e.Record.ConvertRecord(sch)
	sqlGenerator := common_sql.NewSQLGenerator(
		w.opt.Connector,
		sch,
		dataTypes,
	)
	sql, params, err := sqlGenerator.DML(e)
	if err != nil {
		return w.opt.Logger.Errorf("cannot generateDML: %v", err)
	}
	err = w.conn.Exec(sql, params...).Error
	if err != nil {
		return w.opt.Logger.Errorf("cannot execute: %v", err)
	}
	return nil
}

func (w *writer) ExecuteBatch(sourceSchema *schemas.Table, records []core.Event) error {
	tableName := records[0].DestinationTableName
	sch := w.schemaManager.Get(w.opt.Connector.Database, tableName)
	if len(sch.Columns) < 1 {
		w.opt.Logger.Debug("Skipped event, table %s do not exists on the connector", tableName)
		return nil
	}

	convertedRecord := make([]core.EventRecord, len(records))
	for i, record := range records {
		convertedRecord[i] = record.Record.ConvertRecord(sch)
	}

	sql, params, err := batchUpsert(w.opt.Connector, sch, dataTypes, convertedRecord)
	if err != nil {
		return w.opt.Logger.Errorf("cannot generate batch SQL: %v", err)
	}
	err = w.conn.Exec(sql, params...).Error
	if err != nil {
		return w.opt.Logger.Errorf("cannot execute: %v, sql=%s,vals=%+v", err, sql, params)
	}
	w.opt.Logger.Debug("Successfully executed batch SQL,records = %d", len(convertedRecord))
	return nil
}

func (w *writer) processBatch() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	pipeline := w.Pipeline
	w.Pipeline = core.NewPipeline()
	for table, batch := range pipeline.Events {
		w.opt.Logger.Debug("Starting processBatch:%s %d", table, len(batch))
		if err := w.ExecuteBatch(&batch[0].SourceSchema, batch); err != nil {
			return err
		}
	}

	return nil
}

func (w *writer) appendBatch(event core.Event) {
	w.Pipeline.Append("", event)
}
