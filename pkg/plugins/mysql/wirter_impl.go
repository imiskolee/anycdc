package mysql

import (
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
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
	w.connector = connector
	w.conn = db
	return nil
}

func (w *Writer) execute(e core.Event) error {
	sql, params := w.eventToSQL(&e)
	if sql == "" {
		return nil
	}

	err := w.conn.Exec(sql, params...).Error
	if err != nil {
		w.opt.Logger.Error("can not execute event:%s,%s %+v,schema:%+v", err, sql, params)
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
			params = append(params, p)
		}
	}
	if sql == "" {
		return nil
	}
	err := w.conn.Exec(sql, params...).Error
	if err != nil {
		w.opt.Logger.Error("can not execute event:%s,%s", w.opt.Connector, err)
	}
	return err
}

func (w *Writer) eventToSQL(e *core.Event) (string, []interface{}) {
	schema := w.schemaManager.Get(w.connector.Database, e.Table)
	w.opt.Logger.Debug("start convert event %+v,schema=%+v", schema)
	if len(schema.Fields) < 1 {
		w.opt.Logger.Info("Skipped event, table %s do not exists on the connector", e.FullTableName())
		return "", nil
	}
	newEvent := e
	newEvent.Payload = schema.ConvertRecord(newEvent.Payload)
	sql, params := eventToSQL(newEvent)
	return sql, params

}
