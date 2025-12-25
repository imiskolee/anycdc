package mysql

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

func NewWriter(ctx context.Context, opt interface{}) core.Writer {
	o := opt.(*core.WriterOption)
	return &writer{
		opt: o,
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
	sch := w.schemaManager.Get(w.opt.Connector.Database, e.SourceTableName)
	if len(sch.Fields) < 1 {
		w.opt.Logger.Debug("Skipped event, table %s do not exists on the connector", e.SourceTableName)
		return nil
	}
	e.Record = sch.ConvertRecord(e.Record)
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

func (w *writer) ExecuteBatch(tableName string, records []core.EventRecord) error {
	sch := w.schemaManager.Get(w.opt.Connector.Database, tableName)
	if len(sch.Fields) < 1 {
		w.opt.Logger.Debug("Skipped event, table %s do not exists on the connector", tableName)
		return nil
	}
	convertedRecord := make([]core.EventRecord, len(records))
	for i, record := range records {
		convertedRecord[i] = sch.ConvertRecord(record)
	}
	sql, params, err := batchUpsert(w.opt.Connector, sch, dataTypes, convertedRecord)
	if err != nil {
		return w.opt.Logger.Errorf("cannot generate batch SQL: %v", err)
	}
	err = w.conn.Exec(sql, params...).Error
	if err != nil {
		return w.opt.Logger.Errorf("cannot execute: %v", err)
	}
	return nil
}
