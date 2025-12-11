package postgres

import (
	"context"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
)

func (w *Writer) prepare() error {
	connector, err := model.GetConnectorByID(w.opt.Connector)
	if err != nil {
		w.opt.Logger.Error("can not prepare connector:%s,%s", w.opt.Connector, err)
		return err
	}
	db, err := Connect(connector)
	if err != nil {
		w.opt.Logger.Error("can not prepare connector:%s,%s", w.opt.Connector, err)
		return err
	}
	db.Logger = common_sql.NewLogger(w.opt.Logger)
	w.connector = connector
	w.conn = db
	w.schemaManager = core.NewCachedSchemaManager(NewSchema(context.Background(), &core.SchemaOption{
		Connector: w.opt.Connector,
		Logger:    w.opt.Logger,
	}))
	return nil
}

func (w *Writer) execute(e core.Event) error {
	schema := w.schemaManager.Get(w.connector.Database, e.Table)
	if len(schema.Fields) < 1 {
		w.opt.Logger.Info("Skipped event, table %s do not exists on the connector", e.FullTableName())
		return nil
	}
	w.opt.Logger.Debug("start convert event %+v,schema=%+v", schema)
	newEvent := e
	newEvent.Payload = schema.ConvertRecord(newEvent.Payload)
	sql, params := eventToSQL(&newEvent)
	err := w.conn.Exec(sql, params...).Error
	if err != nil {
		w.opt.Logger.Error("can not execute event:%s,%+v,%s", sql, params, err)
	}
	return err
}

func (w *Writer) executeBatch(e []core.Event) error {
	var sql string
	var params []interface{}
	for _, event := range e {
		s, p := w.eventToSQL(&event)
		if s != "" {
			sql = sql + s + ";\n"
			params = append(params, p...)
		}
	}
	if sql == "" {
		return nil
	}
	fmt.Println(sql, params)
	err := w.conn.Exec(sql, params...).Error
	if err != nil {
		w.opt.Logger.Error("can not execute event:%s,%s", w.opt.Connector, err)
	}
	return err
}

func (w *Writer) eventToSQL(e *core.Event) (string, []interface{}) {
	schema := w.schemaManager.Get(w.connector.Database, e.Table)
	if len(schema.Fields) < 1 {
		w.opt.Logger.Info("Skipped event, table %s do not exists on the connector", e.FullTableName())
		return "", nil
	}
	newEvent := e
	newEvent.Payload = schema.ConvertRecord(newEvent.Payload)
	sql, params := eventToSQL(newEvent)
	return sql, params

}
