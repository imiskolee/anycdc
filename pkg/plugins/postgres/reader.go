package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/types"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	uuid "github.com/satori/go.uuid"
	"log"
	"strings"
	"time"
)

type taskExtra struct {
	PublicationName string `json:"publication_name"`
	SlotName        string `json:"slot_name"`
}

type reader struct {
	opt             *core.ReaderOption
	conn            *pgxpool.Pool
	ctx             context.Context
	cancel          context.CancelFunc
	replication     *replication
	latestLSN       pglogrepl.LSN
	latestRealLSN   pglogrepl.LSN
	retries         int
	relations       map[uint32]pglogrepl.RelationMessageV2
	lastHeartBeatAt time.Time
	lastEventAt     *time.Time
	lastSaveAt      time.Time
	schemaManager   core.SchemaManager
}

func newReader(ctx context.Context, opts interface{}) core.Reader {
	c, cancel := context.WithCancel(ctx)
	o := opts.(*core.ReaderOption)
	reader := &reader{
		ctx:        c,
		cancel:     cancel,
		opt:        o,
		lastSaveAt: time.Now(),
		relations:  make(map[uint32]pglogrepl.RelationMessageV2),
		schemaManager: core.NewCachedSchemaManager(newSchema(ctx, &core.SchemaOption{
			Connector: o.Connector,
			Logger:    o.Logger,
		})),
	}
	return reader
}

func (r *reader) initialExtra() (*taskExtra, error) {
	var extra taskExtra
	if r.opt.Task.Extras != "" {
		err := json.Unmarshal([]byte(r.opt.Task.Extras), &extra)
		if err != nil {
			return nil, err
		}
		return &extra, nil
	}
	id := strings.Replace(uuid.NewV4().String(), "-", "", -1)
	shouldUpdate := false
	if extra.PublicationName == "" {
		extra.PublicationName = fmt.Sprintf("anycdc_pub_%s", id)
		shouldUpdate = true
	}
	if extra.SlotName == "" {
		extra.SlotName = fmt.Sprintf("anycdc_slot_%s", id)
		shouldUpdate = true
	}
	if shouldUpdate {
		j, _ := json.Marshal(extra)
		r.opt.Task.Extras = string(j)
		if err := r.opt.Task.PartialUpdates(map[string]interface{}{
			"extras": string(j),
		}); err != nil {
			return nil, err
		}
	}
	return &extra, nil
}

func (r *reader) Prepare() error {
	conn, err := connectPGX(r.opt.Connector)
	if err != nil {
		return r.opt.Logger.Errorf("can not prepare reader connection: %v", err)
	}
	r.conn = conn
	extra, err := r.initialExtra()
	if err != nil {
		return r.opt.Logger.Errorf("can not prepare reader initial extra: %v", err)
	}
	var tables []string
	for _, v := range r.opt.Task.GetTables() {
		tables = append(tables, v.SourceTable)
	}
	r.replication = &replication{
		conn:            conn,
		publicationName: extra.PublicationName,
		slotName:        extra.SlotName,
		tables:          tables,
		logger:          r.opt.Logger,
	}
	return nil
}

func (r *reader) Start() error {
	r.opt.Logger.Info("starting reader for task %s", r.opt.Task.Name)
	defer (func() {
		_ = r.Stop()
	})()
	if err := r.replication.syncPublication(); err != nil {
		return r.opt.Logger.Errorf("can not prepare reader sync publication: %v", err)
	}
	if err := r.replication.syncSlot(); err != nil {
		return r.opt.Logger.Errorf("can not prepare reader sync slot: %v", err)
	}
	if r.opt.Task.LastCDCPosition != "" {
		lsn, err := pglogrepl.ParseLSN(r.opt.Task.LastCDCPosition)
		if err != nil {
			return r.opt.Logger.Errorf("can not parse last cdc position: %v", err)
		}
		r.latestLSN = lsn
	} else {
		lsn, err := r.replication.getLatestPosition()
		if err != nil {
			return r.opt.Logger.Errorf("can not prepare reader last position: %v", err)
		}
		r.latestLSN = lsn
	}
	pluginArgs := []string{
		fmt.Sprintf("publication_names '%s'", r.replication.publicationName),
		"proto_version '1'",
	}
	conn, err := connectReaderRepublication(r.opt.Connector)
	if err != nil {
		return r.opt.Logger.Errorf("can not start reader for task %s: %v", r.opt.Task.Name, err)
	}
	defer conn.Close(context.Background())
	r.latestRealLSN = r.latestLSN
	err = pglogrepl.StartReplication(
		r.ctx,
		conn.PgConn(),
		r.replication.slotName,
		r.latestLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs,
			Mode:       pglogrepl.LogicalReplication,
		},
	)
	if err != nil {
		return r.opt.Logger.Errorf("can not start replication %s", err)
	}
	defer (func() {
		_, err := pglogrepl.SendStandbyCopyDone(context.Background(), conn.PgConn())
		if err != nil {
			r.opt.Logger.Error("failed stop replication,%s", err)
			return
		}
		r.opt.Logger.Info("stopped replication")
	})()
	var loopError error
	for {
		now := time.Now()
		if r.retries > 10 {
			loopError = errors.New("reader stopped,because of too many fails")
			goto end
		}
		select {
		case <-r.ctx.Done():
			goto end
		default:
		}
		if time.Now().Sub(r.lastHeartBeatAt) > 30*time.Second {
			_ = pglogrepl.SendStandbyStatusUpdate(context.Background(),
				conn.PgConn(),
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: r.latestRealLSN,
					ReplyRequested:   false,
					ClientTime:       time.Now(),
				})
			r.lastHeartBeatAt = now
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		msg, err := conn.PgConn().ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue // 超时重试
			}
			r.opt.Logger.Error("failed receive message: %s", err.Error())
			r.retries++
			continue
		}
		for i := 0; i < 10; i++ {
			if loopError = r.handler(msg); loopError != nil {
				r.opt.Logger.Error("failed handler msg: %s", err)
				time.Sleep(time.Duration(i+10) * time.Second)
				continue
			}
			break
		}
		if loopError != nil {
			goto end
		}
		r.retries = 0
	}
end:
	return loopError
}

func (r *reader) Stop() error {
	if r.conn != nil {
		r.conn.Reset()
		r.conn.Close()
		r.conn = nil
	}
	r.cancel()
	time.Sleep(1 * time.Second)

	return nil
}

func (r *reader) handler(msg pgproto3.BackendMessage) error {
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			now := time.Now()
			ev, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if now.Sub(r.lastSaveAt) > time.Duration(r.opt.Task.CDCDelayTime)*time.Minute {
				r.lastSaveAt = now
				r.latestLSN = ev.ServerWALEnd
			}
			if err == nil {
				r.latestRealLSN = ev.ServerWALEnd
				if r.lastEventAt == nil {
					r.lastEventAt = new(time.Time)
				}
				if ev.ServerTime.Unix() != 0 {
					*r.lastEventAt = ev.ServerTime
				}
			}
			break
		case pglogrepl.XLogDataByteID:
			return r.handleXLogData(msg)
		case pglogrepl.StandbyStatusUpdateByteID:
			log.Println("StandbyStatusUpdate", msg.Data)
		}
	}
	return nil
}

func (r *reader) handleXLogData(msg *pgproto3.CopyData) error {
	xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
	if err != nil {
		return r.opt.Logger.Errorf("can not parse XLOG DATA %s", err)
	}
	logicalMsg, err := pglogrepl.ParseV2(xld.WALData, false)
	if err != nil {
		return r.opt.Logger.Errorf("can not parse XLOG DATA %s", err)
	}

	var e core.Event

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		r.relations[logicalMsg.RelationID] = *logicalMsg
		break
	case *pglogrepl.InsertMessageV2:
		rel := r.relations[logicalMsg.RelationID]
		record, err := r.convertToEventRecord(&rel, logicalMsg.InsertMessage.Tuple.Columns)
		if err != nil {
			return r.opt.Logger.Errorf("can not parse insert message into event record %s", err)
		}
		e.Record = record
		e.Type = core.EventTypeInsert
		sch := r.schemaManager.Get(r.opt.Connector.Database, rel.RelationName)
		e.SourceSchema = *sch
		break
	case *pglogrepl.UpdateMessageV2:
		rel := r.relations[logicalMsg.RelationID]
		newData, err := r.convertToEventRecord(&rel, logicalMsg.UpdateMessage.NewTuple.Columns)
		if err != nil {
			return r.opt.Logger.Errorf("can not parse update message into event record %s", err)
		}
		oldData := newData
		if logicalMsg.OldTuple != nil {
			oldData, err = r.convertToEventRecord(&rel, logicalMsg.UpdateMessage.OldTuple.Columns)
			if err != nil {
				return r.opt.Logger.Errorf("can not parse update message into event record %s", err)
			}
		}
		e.Type = core.EventTypeUpdate
		e.Record = newData
		e.OldRecord = new(core.EventRecord)
		*e.OldRecord = oldData
		sch := r.schemaManager.Get(r.opt.Connector.Database, rel.RelationName)
		e.SourceSchema = *sch
		break
	case *pglogrepl.DeleteMessageV2:
		rel := r.relations[logicalMsg.RelationID]
		oldData, err := r.convertToEventRecord(&rel, logicalMsg.DeleteMessage.OldTuple.Columns)
		if err != nil {
			return r.opt.Logger.Errorf("can not parse delete message into event record %s", err)
		}
		e.Type = core.EventTypeDelete
		e.Record = oldData
		sch := r.schemaManager.Get(r.opt.Connector.Database, rel.RelationName)
		e.SourceSchema = *sch
		break
	}
	if e.Type != core.EventTypeUnknown {
		if err := r.opt.Subscriber.ReaderEvent(e); err != nil {
			return r.opt.Logger.Errorf("can not consume event %s", err)
		}
	}
	now := time.Now()
	if now.Sub(r.lastSaveAt) > time.Duration(r.opt.Task.CDCDelayTime)*time.Minute {
		r.lastSaveAt = now
		r.latestLSN = xld.ServerWALEnd
	}
	r.latestRealLSN = xld.ServerWALEnd
	if r.lastEventAt == nil {
		r.lastEventAt = new(time.Time)
	}
	*r.lastEventAt = xld.ServerTime
	return nil
}

func (r *reader) convertToEventRecord(rel *pglogrepl.RelationMessageV2, columns []*pglogrepl.TupleDataColumn) (core.EventRecord, error) {
	var record core.EventRecord
	for idx, col := range columns {
		column := rel.Columns[idx]
		switch col.DataType {
		case pglogrepl.TupleDataTypeToast, pglogrepl.TupleDataTypeText:
			val, err := convertFromPGX(column.DataType, col.Data)
			if err != nil {
				return record, err
			}
			record.Set(column.Name, val)
			break
		case pglogrepl.TupleDataTypeNull:
			record.Set(column.Name, types.NewNullData())
		}
	}
	return record, nil
}

func (r *reader) LatestPosition() core.ReaderPosition {
	lsn, err := r.replication.getLatestPosition()
	if err != nil {
		r.opt.Logger.Error("can not get latest position %s", err)
	}
	return core.ReaderPosition{
		Position: lsn.String(),
	}
}

func (r *reader) CurrentPosition() core.ReaderPosition {
	lsn := r.latestLSN
	return core.ReaderPosition{
		Position:    lsn.String(),
		LastEventAt: r.lastEventAt,
	}
}

func (r *reader) Release() error {
	return r.replication.Release()
}
