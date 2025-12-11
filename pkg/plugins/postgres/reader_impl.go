package postgres

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/types"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"log"
	"strings"
	"time"
)

const (
	ExtraPublicationName = "publication_name"
	ExtraSlotName        = "slot_name"
)

func (s *Reader) prepare() error {
	if s.conn != nil {
		if err := s.conn.Close(context.Background()); err != nil {
			return s.opt.Logger.Errorf("can not stop exists conenction:%s", err.Error())
		}
	}
	connector, err := model.GetConnectorByID(s.opt.Connector)
	if err != nil {
		s.opt.Logger.Error("can not get connector:%s,%s", connector, err)
		return err
	}
	config, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		connector.Username,
		connector.Password,
		connector.Host,
		connector.Port,
		connector.Database,
	))
	if err != nil {
		s.opt.Logger.Error("can not parse connector:%s,%s", connector, err)
		return err
	}
	config.RuntimeParams["replication"] = "database"
	config.TLSConfig = nil
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(s.ctx, config)
	if err != nil {
		s.opt.Logger.Error("can not connect to postgres:%s,%s", connector, err)
		return err
	}
	s.conn = conn

	if err := s.preparePublication(); err != nil {
		s.opt.Logger.Error("can not prepare publication:%s,%s", connector, err)
		return err
	}
	if err := s.prepareSlot(); err != nil {
		s.opt.Logger.Error("can not prepare slot:%s,%s", connector, err)
		return err
	}
	return nil
}

func (s *Reader) prepareSlot() error {
	var exists bool
	err := s.conn.QueryRow(s.ctx, `
		SELECT EXISTS(
			SELECT 1 FROM pg_replication_slots 
			WHERE slot_name = $1 AND slot_type = 'logical'
		)
	`, s.opt.Extra[ExtraSlotName]).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		_, err := s.conn.Exec(s.ctx, fmt.Sprintf(
			"SELECT pg_create_logical_replication_slot('%s', '%s')",
			s.opt.Extra[ExtraSlotName], "pgoutput",
		))
		if err != nil {
			return err
		}
	}
	if s.opt.InitialPosition == "" {
		{
			var currentLSN pglogrepl.LSN
			query := `SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = $1`
			row := s.conn.QueryRow(s.ctx, query, s.opt.Extra[ExtraSlotName])
			_ = row.Scan(&currentLSN)
			s.lastSyncPosition = currentLSN
		}
	} else {
		if err := json.Unmarshal([]byte(s.opt.InitialPosition), &s.lastSyncPosition); err != nil {
			s.opt.Logger.Error("can not parse initial position:%s,%s", s.opt.InitialPosition, err)
			return err
		}
	}
	return nil
}

func (s *Reader) preparePublication() error {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)`
	err := s.conn.QueryRow(s.ctx, query, s.opt.Extra[ExtraPublicationName]).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		sql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
			s.opt.Extra[ExtraPublicationName],
			strings.Join(s.opt.Tables, ","),
		)
		_, err := s.conn.Exec(s.ctx, sql)
		if err != nil {
			return err
		}
	}

	currentTables, err := getPublicationTables(s.ctx, s.conn, s.opt.Extra[ExtraPublicationName].(string))
	if err != nil {
		return err
	}
	needAlert, add, drop := compareTables(currentTables, s.opt.Tables)
	if needAlert {
		if err := alterPublicationTables(s.ctx, s.conn, s.opt.Extra[ExtraPublicationName].(string), add, drop); err != nil {
			return err
		}
	}
	return nil
}

func (s *Reader) start() error {
	s.opt.Logger.Info("starting reader %s from %s", s.opt.Connector, s.lastSyncPosition)
	var successful bool
	defer (func() {
		s.done <- successful
	})()
	pluginArgs := []string{
		fmt.Sprintf("publication_names '%s'", s.opt.Extra[ExtraPublicationName]),
		"proto_version '1'",
	}
	err := pglogrepl.StartReplication(
		s.ctx,
		s.conn.PgConn(),
		s.opt.Extra[ExtraSlotName].(string),
		s.lastSyncPosition,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs,
			Mode:       pglogrepl.LogicalReplication,
		},
	)
	if err != nil {
		s.opt.Logger.Error("can not start reader %s,%s", s.opt.Connector, err)
		return err
	}

	for {
		select {
		case <-s.ctx.Done():
			goto end
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		msg, err := s.conn.PgConn().ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue // 超时重试
			}
			s.opt.Logger.Error("failed receive message %s", err.Error())
		}
		if err := s.handler(msg); err != nil {
			s.opt.Logger.Error("failed handler message %+v, %s", msg, err.Error())
			continue
		}
		_ = pglogrepl.SendStandbyStatusUpdate(context.Background(),
			s.conn.PgConn(),
			pglogrepl.StandbyStatusUpdate{WALWritePosition: s.lastSyncPosition})
	}
end:
	successful = true
	return nil
}

func (s *Reader) handler(msg pgproto3.BackendMessage) error {
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:

			break
		case pglogrepl.XLogDataByteID:
			return s.handleXLogData(msg)
		}
	}
	return nil
}

func (s *Reader) handleXLogData(msg *pgproto3.CopyData) error {
	xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
	if err != nil {
		log.Fatalln("ParseXLogData failed:", err)
		return err
	}
	logicalMsg, err := pglogrepl.ParseV2(xld.WALData, false)
	if err != nil {
		log.Fatal(err)
	}
	var e core.Event

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		s.relations[logicalMsg.RelationID] = *logicalMsg
		break
	case *pglogrepl.InsertMessageV2:
		rel := s.relations[logicalMsg.RelationID]
		data := s.convertDataMap(logicalMsg.RelationID, logicalMsg.Tuple.Columns)
		var pks []core.Field
		for _, k := range getPrimaryKey(rel) {
			pks = append(pks, core.Field{
				Name:  k,
				Value: data[k],
			})
		}
		e = core.Event{
			Type:        core.EventTypeInsert,
			PrimaryKeys: pks,
			Schema:      rel.Namespace,
			Table:       rel.RelationName,
			Payload:     data,
		}
		break
	case *pglogrepl.UpdateMessageV2:
		rel := s.relations[logicalMsg.RelationID]
		newData := s.convertDataMap(logicalMsg.RelationID, logicalMsg.NewTuple.Columns)
		oldData := newData
		if logicalMsg.OldTuple != nil {
			oldData = s.convertDataMap(logicalMsg.RelationID, logicalMsg.OldTuple.Columns)
		}
		var pks []core.Field
		for _, k := range getPrimaryKey(rel) {
			pks = append(pks, core.Field{
				Name:  k,
				Value: oldData[k],
			})
		}
		e = core.Event{
			Type:        core.EventTypeUpdate,
			PrimaryKeys: pks,
			Schema:      rel.Namespace,
			Table:       rel.RelationName,
			Payload:     newData,
		}
		break
	case *pglogrepl.DeleteMessageV2:
		rel := s.relations[logicalMsg.RelationID]

		oldData := s.convertDataMap(logicalMsg.RelationID, logicalMsg.OldTuple.Columns)
		var pks []core.Field
		for _, k := range getPrimaryKey(rel) {
			pks = append(pks, core.Field{
				Name:  k,
				Value: oldData[k],
			})
		}
		e = core.Event{
			Type:        core.EventTypeDelete,
			PrimaryKeys: pks,
			Schema:      rel.Namespace,
			Table:       rel.RelationName,
			Payload:     oldData,
		}
		break
	case *pglogrepl.TruncateMessageV2:
		break
	default:
		break
	}

	if e.Type != 0 {
		if err := s.opt.Subscriber.Event(e); err != nil {
			return err
		}
	}
	s.lastSyncPosition = xld.ServerWALEnd
	return nil
}

func (s *Reader) convertDataMap(relationID uint32, columns []*pglogrepl.TupleDataColumn) map[string]interface{} {
	values := map[string]interface{}{}
	rel := s.relations[relationID]
	for idx, col := range columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeToast, pglogrepl.TupleDataTypeText:
			val := s.convertToTypedData(rel.Columns[idx].DataType, col.Data)
			values[colName] = val
			break
		case pglogrepl.TupleDataTypeNull:
			values[colName] = nil
			break
		}
	}
	return values
}

func (s *Reader) convertToTypedData(oID uint32, data []byte) types.TypedData {
	typ := getBuiltInType(oID)
	if len(data) == 0 {
		return types.NewNullTypedData(typ)
	}
	if typ == types.TypeUnknown {
		return types.NewTypedData(types.TypeString, string(data))
	}
	ot, ok := s.typeMap.TypeForOID(oID)
	if !ok {
		return types.NewTypedData(types.TypeString, string(data))
	}
	if oID == pgtype.UUIDOID || oID == pgtype.TimeOID {
		return types.NewTypedData(types.TypeString, string(data))
	}
	if oID == pgtype.JSONOID {
		return types.NewTypedData(types.TypeJSON, string(data))
	}
	val, err := ot.Codec.DecodeValue(s.typeMap, oID, pgtype.TextFormatCode, data)
	if err != nil {
		return types.NewTypedData(types.TypeString, string(data))
	}

	if valuer, ok := val.(driver.Valuer); ok {
		v, err := valuer.Value()
		if err == nil {
			val = v
		}
	}
	return types.NewTypedData(typ, val)
}

func getPublicationTables(ctx context.Context, pool *pgx.Conn, pubName string) ([]string, error) {
	query := `
		SELECT c.relname 
		FROM pg_publication p
		JOIN pg_publication_rel pr ON p.oid = pr.prpubid
		JOIN pg_class c ON pr.prrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE p.pubname = $1
		ORDER BY 1;
	`
	rows, err := pool.Query(ctx, query, pubName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tbl string
		if err := rows.Scan(&tbl); err != nil {
			return nil, err
		}
		tables = append(tables, tbl)
	}
	return tables, rows.Err()
}

func compareTables(current, target []string) (needAlter bool, add []string, drop []string) {
	currentMap := make(map[string]bool)
	for _, tbl := range current {
		currentMap[tbl] = true
	}
	targetMap := make(map[string]bool)
	for _, tbl := range target {
		targetMap[tbl] = true
	}

	// 找出需要新增的表（目标有，当前无）
	for tbl := range targetMap {
		if !currentMap[tbl] {
			add = append(add, tbl)
		}
	}

	// 找出需要删除的表（当前有，目标无）
	for tbl := range currentMap {
		if !targetMap[tbl] {
			drop = append(drop, tbl)
		}
	}

	needAlter = len(add) > 0 || len(drop) > 0
	return
}

func alterPublicationTables(ctx context.Context, pool *pgx.Conn, pubName string, addTables, dropTables []string) error {
	// 开始事务
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 添加表
	for _, tbl := range addTables {
		query := fmt.Sprintf(`ALTER PUBLICATION %s ADD TABLE %s`,
			quoteIdentifier(pubName),
			quoteIdentifier(tbl),
		)
		if _, err := tx.Exec(ctx, query); err != nil {
			return fmt.Errorf("添加表%s失败: %w", tbl, err)
		}
	}

	// 删除表
	for _, tbl := range dropTables {
		query := fmt.Sprintf(`ALTER PUBLICATION %s DROP TABLE %s`,
			quoteIdentifier(pubName),
			quoteIdentifier(tbl),
		)
		if _, err := tx.Exec(ctx, query); err != nil {
			return fmt.Errorf("删除表%s失败: %w", tbl, err)
		}
	}
	// 提交事务
	return tx.Commit(ctx)
}

func getPrimaryKey(rel pglogrepl.RelationMessageV2) []string {
	var keys []string
	for _, col := range rel.Columns {
		if col.Flags&0x01 != 0 {
			keys = append(keys, col.Name)
		}
	}
	return keys
}

func quoteIdentifier(ident string) string {
	parts := strings.Split(ident, ".")
	for i, part := range parts {
		parts[i] = pgx.Identifier{part}.Sanitize()
	}
	return strings.Join(parts, ".")
}
