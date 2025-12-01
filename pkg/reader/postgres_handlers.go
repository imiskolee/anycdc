package reader

import (
	"bindolabs/anycdc/pkg/event"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"log"
)

func (s *PostgresReader) handler(msg pgproto3.BackendMessage) {
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			s.handlePrimaryKeepaliveMessage(msg)
			break
		case pglogrepl.XLogDataByteID:
			s.handleXLogData(msg)
			break
		}
	}
}

func (s *PostgresReader) handlePrimaryKeepaliveMessage(msg *pgproto3.CopyData) {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
	if err != nil {
		log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
	}
	if pkm.ServerWALEnd > s.clientXLogPos {
		s.clientXLogPos = pkm.ServerWALEnd
	}
}

func (s *PostgresReader) handleXLogData(msg *pgproto3.CopyData) {
	xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
	if err != nil {
		log.Fatalln("ParseXLogData failed:", err)
		return
	}
	logicalMsg, err := pglogrepl.ParseV2(xld.WALData, false)
	if err != nil {
		log.Fatal(err)
	}
	var e event.Event
	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		s.relations[logicalMsg.RelationID] = *logicalMsg
		return
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
		fmt.Println("Updated", logicalMsg)
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
			return
		}
	}
	s.clientXLogPos = xld.ServerWALEnd
}

func (s *PostgresReader) convertDataMap(relationID uint32, columns []*pglogrepl.TupleDataColumn) map[string]interface{} {
	values := map[string]interface{}{}
	rel := s.relations[relationID]
	for idx, col := range columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeToast, pglogrepl.TupleDataTypeText:
			val, err := decodeTextColumnData(s.typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				log.Fatalln("error decoding column data:", err)
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

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		if dataType == pgtype.UUIDOID {
			return string(data), nil
		}
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func getPrimaryKey(rel pglogrepl.RelationMessageV2) string {
	for _, col := range rel.Columns {
		if col.Flags&0x01 != 0 {
			return col.Name
		}
	}
	return ""
}
