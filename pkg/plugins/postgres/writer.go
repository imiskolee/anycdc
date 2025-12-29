package postgres

import (
	"context"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"gorm.io/gorm"
)

type writer struct {
	opt           *core.WriterOption
	conn          *gorm.DB
	schemaManager core.SchemaManager
}

func NewWriter(ctx context.Context, opts interface{}) core.Writer {
	opt := opts.(*core.WriterOption)
	return &writer{
		opt: opt,
	}
}

func (w *writer) Prepare() error {
	conn, err := Connect(w.opt.Connector)
	if err != nil {
		return w.opt.Logger.Errorf("cannot connect to postgres connector: %v", err)
	}
	w.conn = conn
	w.conn.Logger = common_sql.NewLogger(w.opt.Logger)
	w.schemaManager = core.NewCachedSchemaManager(newSchema(context.Background(), &core.SchemaOption{
		Connector: w.opt.Connector,
		Logger:    w.opt.Logger,
	}))
	return nil
}

func (w *writer) Execute(e core.Event) error {
	sch := w.schemaManager.Get(w.opt.Connector.Database, e.SourceTableName)
	if len(sch.Columns) < 1 {
		w.opt.Logger.Debug("Skipped event, table %s do not exists on the connector", e.SourceTableName)
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

func (w *writer) ExecuteBatch(table string, records []core.EventRecord) error {
	sch := w.schemaManager.Get(w.opt.Connector.Database, table)
	if len(sch.Columns) < 1 {
		w.opt.Logger.Debug("Skipped event, table %s do not exists on the connector", table)
		return nil
	}
	convertedRecord := make([]core.EventRecord, len(records))
	for i, record := range records {
		convertedRecord[i] = record.ConvertRecord(sch)
	}
	sql, params, err := batchUpsert(sch, dataTypes, convertedRecord)
	if err != nil {
		return w.opt.Logger.Errorf("cannot generate batch SQL: %v", err)
	}
	err = w.conn.Exec(sql, params...).Error
	if err != nil {
		return w.opt.Logger.Errorf("cannot execute: %v", err)
	}
	return nil
}
