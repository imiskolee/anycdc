package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"time"
)

const (
	extraParamServerID string = "server_id"
)

func (s *Reader) prepare() error {
	conenctor, err := model.GetConnectorByID(s.opt.Connector)
	if err != nil {
		s.opt.Logger.Error("failed to prepare connector %s, %s", s.opt.Connector, err.Error())
		return err
	}
	s.connector = conenctor
	return nil
}

func (s *Reader) start() {
	s.opt.Logger.Info("starting reader %s on %s", s.opt.Connector, s.opt.InitialPosition)
	successful := false
	s.running = true
	defer (func() {
		s.running = false
		s.done <- successful
	})()
	if s.connector == nil {
		s.opt.Logger.Error("failed to start reader %s, can not get connector.", s.opt.Connector)
		return
	}
	serverID, ok := s.opt.Extra[extraParamServerID].(float64)
	if !ok {
		s.opt.Logger.Error("failed to convert extra param server id to int")
		return
	}
	s.binlogCfg = replication.BinlogSyncerConfig{
		Host:                 s.connector.Host,
		Port:                 uint16(s.connector.Port),
		User:                 s.connector.Username,
		Password:             s.connector.Password,
		Charset:              "utf8mb4",
		ServerID:             uint32(serverID), // 伪从库 ID（必须唯一，不能与主库/其他从库重复）
		Flavor:               "mariadb",        // 数据库类型（mysql/mariadb）
		ParseTime:            true,
		UseDecimal:           true,
		MaxReconnectAttempts: 100,
		HeartbeatPeriod:      60 * time.Second,
	}
	s.syncer = replication.NewBinlogSyncer(s.binlogCfg)
	defer s.syncer.Close()

	if s.opt.InitialPosition != "" {
		if err := json.Unmarshal([]byte(s.opt.InitialPosition), &s.currentPos); err != nil {
			s.opt.Logger.Error("failed to parse last position, %s", err.Error())
			return
		}
	}

	streamer, err := s.syncer.StartSync(s.currentPos)
	if err != nil {
		s.opt.Logger.Error("failed to start syncer, %s", err.Error())
		return
	}
	for {
		select {
		case <-s.ctx.Done():
			s.opt.Logger.Info("successfully stopped reader %s", s.opt.Connector)
			break
		default:
		}
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
		event, err := streamer.GetEvent(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			s.opt.Logger.Debug("get event timeout, %s", s.opt.Connector)
			continue
		}
		cancel()
		if err != nil {
			s.opt.Logger.Error("failed to reader event,%s", err.Error())
			time.Sleep(1 * time.Second)
			continue
		}
		if err := s.handler(event); err != nil {
			s.opt.Logger.Error("failed to handle event,%s", err.Error())
			break
		}
		if event.Header.EventType == replication.XID_EVENT {
			s.currentPos = s.syncer.GetNextPosition()
		}
	}
	successful = true
}

func (s *Reader) stop() error {
	if !s.running {
		return nil
	}
	s.cancel()
	res := <-s.done
	if res {
		return nil
	}
	return errors.New("failed to stop reader " + s.opt.Connector)

}

func (s *Reader) handler(e *replication.BinlogEvent) error {
	s.opt.Logger.Debug("event_type=%d timestamp=%d", e.Header.EventType, e.Header.Timestamp)
	s.lastEventAt = time.Unix(int64(e.Header.Timestamp), 0)
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2:
		rowsEvent, ok := e.Event.(*replication.RowsEvent)
		if !ok {
			return s.opt.Logger.Errorf("can not convert %v to RowsEvent", e.Event)
		}
		dbName := string(rowsEvent.Table.Schema)
		tableName := string(rowsEvent.Table.Table)
		s.opt.Logger.Debug("event_type=%d db=%s table=%s, timestamp=%d", e.Header.EventType, dbName, tableName, e.Header.Timestamp)
		if dbName != s.connector.Database {
			s.opt.Logger.Debug("skipped event,because of db %s can not match %s", dbName, s.connector.Database)
			return nil
		}
		filtered := false
		for _, v := range s.opt.Tables {
			if v == tableName {
				filtered = true
				break
			}
		}
		if !filtered {
			s.opt.Logger.Debug("table %s not configured", tableName)
			return nil
		}

		table := s.schemaManager.Get(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
		records := s.rowsToEntry(table, rowsEvent)
		keys := table.GetPrimaryKeys()
		eventType := core.EventTypeInsert
		if e.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
			eventType = core.EventTypeUpdate
		}
		for _, record := range records {
			var pks []core.Field
			for _, k := range keys {
				pks = append(pks, core.Field{
					Name:  k,
					Value: record[k],
				})
			}
			err := s.opt.Subscriber.Event(core.Event{
				Schema:      string(rowsEvent.Table.Schema),
				Table:       string(rowsEvent.Table.Table),
				PrimaryKeys: pks,
				Type:        eventType,
				Payload:     record,
			})
			if err != nil {
				return err
			}
		}
		break
	}
	return nil
}

func (s *Reader) rowsToEntry(schema *core.SimpleTableSchema, binlog *replication.RowsEvent) []map[string]interface{} {
	var records []map[string]interface{}
	for _, row := range binlog.Rows {
		record := make(map[string]interface{})
		for idx, col := range row {
			field, _ := schema.GetFieldByIndex(uint(idx))
			builtType := getBuiltType(field.Type)
			td := common_sql.ConvertToBuiltInTypedData(builtType, col)
			record[field.Name] = td
		}
		records = append(records, record)
	}
	return records
}
