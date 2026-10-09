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
	"strings"
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
	lastSaveAt     time.Time
	lastCompleteAt time.Time
}

func NewReader(ctx context.Context, opt interface{}) core.Reader {
	c, cancel := context.WithCancel(ctx)
	o := opt.(*core.ReaderOption)
	return &reader{
		ctx:            c,
		cancel:         cancel,
		opt:            o,
		lastSaveAt:     time.Now(),
		lastCompleteAt: time.Now(),
		done:           make(chan bool),
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
		Flavor:               r.flavor(),             // 数据库类型（mysql/mariadb）
		ParseTime:            true,
		UseDecimal:           false,
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
	if err := json.Unmarshal([]byte(latestPosition), &r.latestPosition); err != nil {
		r.opt.Logger.Error("can not parse last cdc position: %v", err)
		return err
	}
	startPosition := r.latestPosition
	if startPosition.Pos < 4 {
		// binlog 文件固定 4 字节起始头，兜底到文件头之后；否则使用保存的真实偏移续读，
		// 避免每次重启都从当前文件开头重放已有事件。
		startPosition.Pos = 4
	}
	streamer, err := r.syncer.StartSync(startPosition)
	lastSyncTime := time.Now()
	if err != nil {
		r.opt.Logger.Error("failed to start syncer, %s", err.Error())
		return err
	}
	for {
		now := time.Now()
		if r.retries > 10 {
			return r.opt.Logger.Errorf("reader stopped,because of too many retries")
		}
		select {
		case <-r.ctx.Done():
			r.opt.Logger.Info("successfully stopped reader %s", r.opt.Connector.Name)
			goto end
		default:
		}
		ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
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
		for i := 0; i < 10; i++ {
			if err = r.handler(event); err != nil {
				r.retries++
				r.opt.Logger.Error("failed to handle event,%s", err.Error())
				time.Sleep(time.Duration((i+1)*10) * time.Second)
				continue
			}
			break
		}
		if err != nil {
			goto end
		}
		r.retries = 0
		r.lastCompleteAt = time.Now()
		if event.Header.EventType == replication.XID_EVENT {
			if r.lastCompleteAt.Sub(lastSyncTime) > (time.Duration(r.opt.Task.CDCDelayTime))*time.Minute {
				lastSyncTime = now
				r.latestPosition = r.syncer.GetNextPosition()
			}
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
	var ret struct {
		File     string `gorm:"column:file"`
		Position uint32 `gorm:"column:position"`
	}
	err := r.conn.Raw("SHOW MASTER STATUS").Find(&ret).Error
	if err != nil {
		// MySQL 8.4+ 已移除 SHOW MASTER STATUS，回退到 SHOW BINARY LOG STATUS
		err = r.conn.Raw("SHOW BINARY LOG STATUS").Find(&ret).Error
	}
	if err != nil {
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
			if v.SourceTable == tableName {
				shouldRun = true
				break
			}
		}
		if !shouldRun {
			return nil
		}
		table := r.schemaManager.Get(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
		isUpdate := e.Header.EventType == replication.UPDATE_ROWS_EVENTv0 ||
			e.Header.EventType == replication.UPDATE_ROWS_EVENTv1 ||
			e.Header.EventType == replication.UPDATE_ROWS_EVENTv2
		if isUpdate {
			// binlog UPDATE 事件的 Rows 以 [前像, 后像] 成对出现，逐对生成一个事件
			for i := 0; i+1 < len(rowsEvent.Rows); i += 2 {
				before := r.decodeRow(table, rowsEvent.Rows[i])
				after := r.decodeRow(table, rowsEvent.Rows[i+1])
				ev := &core.Event{
					SourceSchema: *table,
					Type:         core.EventTypeUpdate,
					Record:       after,
					OldRecord:    &before,
				}
				pos, _ := json.Marshal(r.syncer.GetNextPosition())
				ev.LastPOS = string(pos)
				if err := r.opt.Subscriber.ReaderEvent(*ev); err != nil {
					return err
				}
			}
		} else {
			for _, row := range rowsEvent.Rows {
				ev := &core.Event{
					SourceSchema: *table,
					Type:         core.EventTypeInsert,
					Record:       r.decodeRow(table, row),
				}
				pos, _ := json.Marshal(r.syncer.GetNextPosition())
				ev.LastPOS = string(pos)
				if err := r.opt.Subscriber.ReaderEvent(*ev); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// decodeRow 将 binlog 中的一行解码为 core.EventRecord。
func (s *reader) decodeRow(schema *schemas.Table, row []interface{}) core.EventRecord {
	var record core.EventRecord
	for idx, col := range row {
		field, _ := schema.GetFieldByIndex(uint(idx))
		td, err := dataTypes.Encode(field.DataType, col)
		if err == nil {
			record.Set(field.Name, td)
		}
	}
	return record
}

// flavor 根据连接器类型决定 binlog 解析的 dialect，MySQL 必须为 mysql，否则 GTID 事件与字符集识别错误。
func (r *reader) flavor() string {
	if strings.Contains(strings.ToLower(r.opt.Connector.Type), "mariadb") {
		return "mariadb"
	}
	return "mysql"
}

func (s *reader) Release() error {
	return nil
}
