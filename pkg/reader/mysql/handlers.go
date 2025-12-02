package mysql

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/entry"
	"bindolabs/anycdc/pkg/event"
	"errors"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"os"
)

var typesMapping = map[entry.Type][]int{
	entry.TypeNumeric:   []int{schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT, schema.TYPE_DECIMAL, schema.TYPE_FLOAT},
	entry.TypeString:    []int{schema.TYPE_STRING},
	entry.TypeJSON:      []int{schema.TYPE_JSON},
	entry.TypeTimestamp: []int{schema.TYPE_DATETIME},
	entry.TypeDate:      []int{schema.TYPE_DATE},
}

func getBuiltType(mysqlType int) entry.Type {
	for k, tt := range typesMapping {
		for _, t := range tt {
			if t == mysqlType {
				return k
			}
		}
	}
	return entry.TypeUnknown
}

func (s *MySQLReader) handle(binlog *replication.BinlogEvent) error {
	connector, _ := config.GetConnector(s.conf.Connector)
	switch binlog.Header.EventType {
	case replication.TABLE_MAP_EVENT:
		tableEvent := binlog.Event.(*replication.TableMapEvent)
		tableEvent.Dump(os.Stdout)
		break
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
			_ = s.opt.Subscriber.Consume(event.Event{
				Schema:          string(rowsEvent.Table.Schema),
				Table:           string(rowsEvent.Table.Table),
				PrimaryKey:      pk,
				PrimaryKeyValue: pkValue,
				Type:            eventType,
				Payload:         record,
			})
		}
		break
	}
	return nil
}

func (s *MySQLReader) rowsToEntry(binlog *replication.RowsEvent) []map[string]interface{} {
	types := binlog.Table.ColumnType
	schema, err := s.schema.GetTable(string(binlog.Table.Schema), string(binlog.Table.Table))
	if err != nil {
		panic(err)
	}
	var records []map[string]interface{}
	for _, row := range binlog.Rows {
		record := make(map[string]interface{})
		for idx, col := range row {
			field, _ := schema.GetFieldByIndex(uint(idx))
			colType := types[idx]
			builtType := getBuiltType(int(colType))
			td := entry.NewTypedData(builtType, col)
			record[field.Name] = td
		}
		records = append(records, record)
	}
	return records
}
