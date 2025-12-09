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
	schema := w.schemaManager.Get(w.connector.Database, e.Table)
	newEvent := e
	newEvent.Payload = schema.ConvertRecord(newEvent.Payload)
	sql, params := eventToSQL(&newEvent)
	err := w.conn.Exec(sql, params...).Error
	if err != nil {
		w.opt.Logger.Error("can not execute event:%s,%s", w.opt.Connector, err)
	}
	return err
}
