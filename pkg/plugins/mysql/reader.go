package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"gorm.io/gorm"
	"math/rand"
	"time"
)

type extra struct {
	ServerID int `json:"server_id"`
}

type reader struct {
	opt            *core.ReaderOption
	binlogCfg      replication.BinlogSyncerConfig
	syncer         *replication.BinlogSyncer
	ctx            context.Context
	cancel         context.CancelFunc
	latestPosition mysql.Position
	schemaManager  core.SchemaManager
	running        bool
	done           chan bool
	retries        int
	conn           *gorm.DB
	lastEventAt    *time.Time
}

func NewReader(ctx context.Context, opt interface{}) core.Reader {
	c, cancel := context.WithCancel(ctx)
	o := opt.(*core.ReaderOption)
	return &reader{
		ctx:    c,
		cancel: cancel,
		opt:    o,
		done:   make(chan bool),
		schemaManager: core.NewCachedSchemaManager(NewSchema(ctx, &core.SchemaOption{
			Connector: o.Connector,
			Logger:    o.Logger,
		})),
	}
}

func (r *reader) initialExtra() (extra, error) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	var e extra
	if r.opt.Task.Extras != "" {
		err := json.Unmarshal([]byte(r.opt.Task.Extras), &e)
		if err != nil {
			return e, err
		}
		return e, nil
	}
	e.ServerID = rnd.Intn(1 << 32)
	j, _ := json.Marshal(e)
	r.opt.Task.Extras = string(j)
	if err := r.opt.Task.PartialUpdates(map[string]interface{}{
		"extras": string(j),
	}); err != nil {
		return e, err
	}
	return e, nil
}

func (r *reader) Prepare() error {
	db, err := Connect(r.opt.Connector)
	if err != nil {
		return err
	}
	r.conn = db
	extra, err := r.initialExtra()
	if err != nil {
		return r.opt.Logger.Errorf("can not prepare reader initial extra: %v", err)
	}
	r.binlogCfg = replication.BinlogSyncerConfig{
		Host:                 r.opt.Connector.Host,
		Port:                 uint16(r.opt.Connector.Port),
		User:                 r.opt.Connector.Username,
		Password:             r.opt.Connector.Password,
		Charset:              "utf8mb4",
		ServerID:             uint32(extra.ServerID), // 伪从库 ID（必须唯一，不能与主库/其他从库重复）
		Flavor:               "mariadb",              // 数据库类型（mysql/mariadb）
		ParseTime:            true,
		UseDecimal:           true,
		MaxReconnectAttempts: 100,
		HeartbeatPeriod:      60 * time.Second,
	}
	return nil
}

func (r *reader) Start() error {
	successful := false
	r.running = true
	defer (func() {
		r.syncer.Close()
		r.running = false
		r.done <- successful
	})()
	r.syncer = replication.NewBinlogSyncer(r.binlogCfg)
	latestPosition := r.opt.Task.LastCDCPosition
	if latestPosition == "" {
		latestPosition = r.LatestPosition().Position
	}
	if err := json.Unmarshal([]byte(r.opt.Task.LastCDCPosition), &r.latestPosition); err != nil {
		r.opt.Logger.Error("can not parse last cdc position: %v", err)
		return err
	}
	streamer, err := r.syncer.StartSync(r.latestPosition)
	if err != nil {
		r.opt.Logger.Error("failed to start syncer, %s", err.Error())
		return err
	}
	for {
		if r.retries > 10 {
			return r.opt.Logger.Errorf("reader stopped,because of too many retries")
		}
		select {
		case <-r.ctx.Done():
			r.opt.Logger.Info("successfully stopped reader %s", r.opt.Connector.Name)
			goto end
		default:
		}
		ctx, cancel := context.WithTimeout(r.ctx, 10*time.Second)
		event, err := streamer.GetEvent(ctx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if errors.Is(err, context.Canceled) {
			r.opt.Logger.Info("successfully stopped reader %s", r.opt.Connector.Name)
			goto end
		}
		if err != nil {
			r.retries++
			r.opt.Logger.Error("failed to reader event,%s", err.Error())
			time.Sleep(time.Duration(r.retries) * time.Second)
			continue
		}
		if err := r.handler(event); err != nil {
			r.retries++
			r.opt.Logger.Error("failed to handle event,%s", err.Error())
			time.Sleep(time.Duration(r.retries) * time.Second)
			break
		}
		r.retries = 0
		if event.Header.EventType == replication.XID_EVENT {
			r.latestPosition = r.syncer.GetNextPosition()
			pt := time.Unix(int64(event.Header.Timestamp), 0)
			r.lastEventAt = &pt
		}
	}
end:
	successful = true
	return nil
}

func (r *reader) Stop() error {
	r.opt.Logger.Info("starting stop reader %s", r.opt.Connector.Name)
	if !r.running {
		return nil
	}
	r.cancel()
	res := <-r.done
	if res {
		return nil
	}
	return nil
}

func (r *reader) LatestPosition() core.ReaderPosition {
	sql := "SHOW MASTER STATUS"
	var ver string
	if err := r.conn.Raw("SELECT VERSION()").Scan(&ver).Error; err != nil {
		r.opt.Logger.Error("can not get latest master position: %v", err)
		return core.ReaderPosition{}
	}
	if ver > "8.0.34" {
		sql = "SHOW BINARY LOG STATUS"
	}
	var ret struct {
		File     string `gorm:"column:file"`
		Position uint32 `gorm:"column:position"`
	}
	if err := r.conn.Raw(sql).Find(&ret).Error; err != nil {
		r.opt.Logger.Error("can not get latest master position: %v", err)
		return core.ReaderPosition{}
	}
	pos := mysql.Position{
		Name: ret.File,
		Pos:  ret.Position,
	}
	j, _ := json.Marshal(pos)
	return core.ReaderPosition{Position: string(j), LastEventAt: r.lastEventAt}
}

func (r *reader) CurrentPosition() core.ReaderPosition {
	j, _ := json.Marshal(r.latestPosition)
	return core.ReaderPosition{Position: string(j), LastEventAt: r.lastEventAt}
}

func (r *reader) handler(e *replication.BinlogEvent) error {
	switch e.Header.EventType {
	case
		replication.WRITE_ROWS_EVENTv0,
		replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:
		rowsEvent, ok := e.Event.(*replication.RowsEvent)
		if !ok {
			return r.opt.Logger.Errorf("can not convert %v to RowsEvent", e.Event)
		}
		dbName := string(rowsEvent.Table.Schema)
		tableName := string(rowsEvent.Table.Table)
		if dbName != r.opt.Connector.Database {
			return nil
		}
		shouldRun := false
		for _, v := range r.opt.Task.GetTables() {
			if v == tableName {
				shouldRun = true
				break
			}
		}
		if !shouldRun {
			return nil
		}

		table := r.schemaManager.Get(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
		records := r.rowsToEntry(table, rowsEvent)
		for _, record := range records {
			var ev core.Event
			ev.Record = record
			ev.SourceDatabase = dbName
			ev.SourceTableName = tableName
			ev.Record = record
			if e.Header.EventType == replication.UPDATE_ROWS_EVENTv0 ||
				e.Header.EventType == replication.UPDATE_ROWS_EVENTv1 ||
				e.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
				ev.Type = core.EventTypeUpdate
				ev.OldRecord = new(core.EventRecord)
				*ev.OldRecord = ev.Record
			} else {
				ev.Type = core.EventTypeInsert
			}
			if err := r.opt.Subscriber.ReaderEvent(ev); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *reader) rowsToEntry(schema *schemas.Table, binlog *replication.RowsEvent) []core.EventRecord {
	var records []core.EventRecord
	for _, row := range binlog.Rows {
		var record core.EventRecord
		for idx, col := range row {
			field, _ := schema.GetFieldByIndex(uint(idx))
			td, err := dataTypes.Encode(field.DataType, col)
			if err == nil {
				record.Set(field.Name, td)
			}
		}
		records = append(records, record)
	}
	return records
}

func (s *reader) Release() error {
	return nil
}
