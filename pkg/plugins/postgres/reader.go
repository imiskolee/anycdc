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
	retries         int
	relations       map[uint32]pglogrepl.RelationMessageV2
	lastHeartBeatAt time.Time
}

func newReader(ctx context.Context, opts interface{}) core.Reader {
	c, cancel := context.WithCancel(ctx)
	reader := &reader{
		ctx:       c,
		cancel:    cancel,
		opt:       opts.(*core.ReaderOption),
		relations: make(map[uint32]pglogrepl.RelationMessageV2),
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
	r.replication = &replication{
		conn:            conn,
		publicationName: extra.PublicationName,
		slotName:        extra.SlotName,
		tables:          r.opt.Task.GetTables(),
		logger:          r.opt.Logger,
	}
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
	return nil
}

func (r *reader) Start() error {
	r.opt.Logger.Info("starting reader for task %s", r.opt.Task.Name)
	pluginArgs := []string{
		fmt.Sprintf("publication_names '%s'", r.replication.publicationName),
		"proto_version '1'",
	}
	conn, err := r.conn.Acquire(r.ctx)
	if err != nil {
		return r.opt.Logger.Errorf("can not start reader for task %s: %v", r.opt.Task.Name, err)
	}
	err = pglogrepl.StartReplication(
		r.ctx,
		conn.Conn().PgConn(),
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
		_, err := pglogrepl.SendStandbyCopyDone(context.Background(), conn.Conn().PgConn())
		if err != nil {
			r.opt.Logger.Error("failed stop replication,%s", err)
			return
		}
		r.opt.Logger.Info("stopped replication")
		r.conn = nil
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
		if now.Sub(r.lastHeartBeatAt) > 30*time.Second {
			_ = pglogrepl.SendStandbyStatusUpdate(context.Background(),
				conn.Conn().PgConn(),
				pglogrepl.StandbyStatusUpdate{WALWritePosition: r.latestLSN})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		msg, err := conn.Conn().PgConn().ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue // 超时重试
			}
			r.opt.Logger.Error("failed receive message: %s", err.Error())
			r.retries++
			continue
		}
		if err := r.handler(msg); err != nil {
			r.opt.Logger.Error("failed handler msg: %s", err)
			r.retries++
			continue
		}
		r.retries = 0
	}
end:
	return loopError
}

func (r *reader) Stop() error {
	r.cancel()
	time.Sleep(1 * time.Second)
	return nil
}

func (r *reader) handler(msg pgproto3.BackendMessage) error {
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:

			break
		case pglogrepl.XLogDataByteID:
			return r.handleXLogData(msg)
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
		e.SourceDatabase = r.opt.Connector.Database
		e.SourceTableName = rel.RelationName
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
			return r.opt.Logger.Errorf("can not parse update message into event record %s", err)
		}
		e.Type = core.EventTypeUpdate
		e.SourceDatabase = r.opt.Connector.Database
		e.SourceTableName = rel.RelationName
		e.Record = newData
		e.OldRecord = new(core.EventRecord)
		*e.OldRecord = oldData
		break
	case *pglogrepl.DeleteMessageV2:
		rel := r.relations[logicalMsg.RelationID]
		oldData, err := r.convertToEventRecord(&rel, logicalMsg.DeleteMessage.OldTuple.Columns)
		if err != nil {
			return r.opt.Logger.Errorf("can not parse delete message into event record %s", err)
		}
		e.Type = core.EventTypeDelete
		e.SourceDatabase = r.opt.Connector.Database
		e.SourceTableName = rel.RelationName
		e.Record = oldData
		break
	}
	if e.Type != core.EventTypeUnknown {
		if err := r.opt.Subscriber.ReaderEvent(e); err != nil {
			return r.opt.Logger.Errorf("can not consume event %s", err)
		}
	}
	r.latestLSN = xld.ServerWALEnd
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

func (r *reader) LatestPosition() string {
	lsn, err := r.replication.getLatestPosition()
	if err != nil {
		r.opt.Logger.Error("can not get latest position %s", err)
	}
	return lsn.String()
}

func (r *reader) CurrentPosition() string {
	return r.latestLSN.String()
}
