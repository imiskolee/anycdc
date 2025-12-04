package mysql

import (
	"errors"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/entry"
	"github.com/imiskolee/anycdc/pkg/event"
)

var typesMapping = map[entry.Type][]string{
	entry.TypeNumeric: {ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt,
		ColumnTypeBigInt, ColumnTypeDecimal, ColumnTypeFloat, ColumnTypeDouble, ColumnTypeReal, ColumnTypeNumeric},
	entry.TypeString: {ColumnTypeVarchar, ColumnTypeChar, ColumnTypeLineString, ColumnTypeMultiLineString,
		ColumnTypeTinyText, ColumnTypeText, ColumnTypeMediumText, ColumnTypeLongText},
	entry.TypeJSON:      {ColumnTypeJSON},
	entry.TypeTimestamp: {ColumnTypeDateTime, ColumnTypeTimestamp},
	entry.TypeDate:      {ColumnTypeDate},
}

func getBuiltType(mysqlType string) entry.Type {
	for k, tt := range typesMapping {
		for _, t := range tt {
			if t == mysqlType {
				return k
			}
		}
	}
	return entry.TypeUnknown
}

func (s *Reader) handle(binlog *replication.BinlogEvent) error {
	connector, _ := config.GetConnector(s.conf.Connector)
	switch binlog.Header.EventType {
	case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2:
		rowsEvent, ok := binlog.Event.(*replication.RowsEvent)
		if !ok {
			return errors.New("unexpected event type")
		}
		dbName := string(rowsEvent.Table.Schema)
		tableName := string(rowsEvent.Table.Table)
		if dbName != connector.Database {
			return nil
		}
		filtered := false
		for _, v := range s.conf.Tables {
			if v == tableName {
				filtered = true
			}
		}
		if !filtered {
			return nil
		}
		table, _ := s.schema.GetTable(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
		records := s.rowsToEntry(rowsEvent)
		keys := table.GetPrimaryKeys()
		pk := ""
		eventType := event.TypeInsert
		if binlog.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
			eventType = event.TypeUpdate
		}
		var pkValue interface{}
		for _, record := range records {
			if len(keys) > 0 {
				pk = keys[0]
				pkValue = record[pk]
			}
			err := s.opt.Subscriber.Consume(event.Event{
				Schema:          string(rowsEvent.Table.Schema),
				Table:           string(rowsEvent.Table.Table),
				PrimaryKey:      pk,
				PrimaryKeyValue: pkValue,
				Type:            eventType,
				Payload:         record,
			})
			if err != nil {
				return err
			}
		}
		break
	}
	return nil
}

func (s *Reader) rowsToEntry(binlog *replication.RowsEvent) []map[string]interface{} {
	schema, err := s.schema.GetTable(string(binlog.Table.Schema), string(binlog.Table.Table))
	if err != nil {
		return nil
	}
	var records []map[string]interface{}
	for _, row := range binlog.Rows {
		record := make(map[string]interface{})
		for idx, col := range row {
			field, _ := schema.GetFieldByIndex(uint(idx))
			builtType := getBuiltType(field.Type)
			td := entry.NewTypedData(builtType, col)
			record[field.Name] = td
		}
		records = append(records, record)
	}
	return records
}
