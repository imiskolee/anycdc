package postgres

import (
	"github.com/imiskolee/anycdc/pkg/event"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"log"
)

func (s *PostgresReader) handler(msg pgproto3.BackendMessage) error {
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			s.handlePrimaryKeepaliveMessage(msg)
			break
		case pglogrepl.XLogDataByteID:
			return s.handleXLogData(msg)
			break
		}
	}
	return nil
}

func (s *PostgresReader) handlePrimaryKeepaliveMessage(msg *pgproto3.CopyData) {
	/*
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
		}
		if pkm.ServerWALEnd > s.clientXLogPos {
			s.clientXLogPos = pkm.ServerWALEnd
		}
	*/
}

func (s *PostgresReader) handleXLogData(msg *pgproto3.CopyData) error {
	xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
	if err != nil {
		log.Fatalln("ParseXLogData failed:", err)
		return err
	}
	logicalMsg, err := pglogrepl.ParseV2(xld.WALData, false)
	if err != nil {
		log.Fatal(err)
	}
	var e event.Event
	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		s.relations[logicalMsg.RelationID] = *logicalMsg
		return nil
	case *pglogrepl.InsertMessageV2:
		rel := s.relations[logicalMsg.RelationID]
		data := s.convertDataMap(logicalMsg.RelationID, logicalMsg.Tuple.Columns)
		e = event.Event{
			Type:       event.TypeInsert,
			PrimaryKey: getPrimaryKey(rel),
			Schema:     rel.Namespace,
			Table:      rel.RelationName,
			Payload:    data,
		}
		break
	case *pglogrepl.UpdateMessageV2:
		rel := s.relations[logicalMsg.RelationID]
		pk := getPrimaryKey(rel)
		newData := s.convertDataMap(logicalMsg.RelationID, logicalMsg.NewTuple.Columns)
		oldData := newData
		if logicalMsg.OldTuple != nil {
			oldData = s.convertDataMap(logicalMsg.RelationID, logicalMsg.OldTuple.Columns)
		}
		e = event.Event{
			Type:            event.TypeUpdate,
			PrimaryKey:      pk,
			PrimaryKeyValue: oldData[pk],
			Schema:          rel.Namespace,
			Table:           rel.RelationName,
			Payload:         newData,
		}
		break
	case *pglogrepl.DeleteMessageV2:
		rel := s.relations[logicalMsg.RelationID]
		pk := getPrimaryKey(rel)
		oldData := s.convertDataMap(logicalMsg.RelationID, logicalMsg.OldTuple.Columns)
		e = event.Event{
			Type:            event.TypeDelete,
			PrimaryKey:      pk,
			PrimaryKeyValue: oldData[pk],
			Schema:          rel.Namespace,
			Table:           rel.RelationName,
			Payload:         oldData,
		}
		break
	case *pglogrepl.TruncateMessageV2:
		break
	default:
		break
	}

	if e.Type != 0 {
		if err := s.opt.Subscriber.Consume(e); err != nil {
			log.Fatal("Subscriber consume failed:", err)
			return err
		}
	}
	s.clientXLogPos = xld.ServerWALEnd
	return nil
}

func (s *PostgresReader) convertDataMap(relationID uint32, columns []*pglogrepl.TupleDataColumn) map[string]interface{} {
	values := map[string]interface{}{}
	rel := s.relations[relationID]
	for idx, col := range columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeToast, pglogrepl.TupleDataTypeText:
			val, err := convertToTypedData(s.typeMap, rel.Columns[idx].DataType, col.Data)
			if err != nil {
				panic(err)
			}
			values[colName] = val
			break
		case pglogrepl.TupleDataTypeNull:
			values[colName] = nil
			break
		}
	}
	return values
}

func getPrimaryKey(rel pglogrepl.RelationMessageV2) string {
	for _, col := range rel.Columns {
		if col.Flags&0x01 != 0 {
			return col.Name
		}
	}
	return ""
}
