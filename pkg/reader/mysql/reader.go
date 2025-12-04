package mysql

import (
	"context"
	"encoding/json"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/imiskolee/anycdc/pkg/common"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/logs"
	"github.com/imiskolee/anycdc/pkg/reader"
	"github.com/imiskolee/anycdc/pkg/schema"
	"strconv"
	"time"
)

const (
	extraParamServerID string = "server_id"
)

func init() {
	reader.Register(config.ConnectorTypeMySQL, NewReader)
}

type Reader struct {
	conf       config.Reader
	schema     *schema.Manager
	opt        reader.ReaderOptions
	binlogCfg  replication.BinlogSyncerConfig
	ctx        context.Context
	syncer     *replication.BinlogSyncer
	currentPos mysql.Position
	cancel     context.CancelFunc
}

func NewReader(conf config.Reader, opt *reader.ReaderOptions) reader.Reader {
	ctx, cancel := context.WithCancel(context.TODO())
	return &Reader{
		conf:   conf,
		opt:    *opt,
		ctx:    ctx,
		cancel: cancel,
		schema: schema.NewManager(conf.Connector, common.SyncSchema),
	}
}

func (s *Reader) connect() {
	connector, _ := config.GetConnector(s.conf.Connector)
	serverID, _ := strconv.ParseInt(s.conf.Extras[extraParamServerID], 10, 64)
	s.binlogCfg = replication.BinlogSyncerConfig{
		Host:                 connector.Host,
		Port:                 uint16(connector.Port),
		User:                 connector.Username,
		Password:             connector.Password,
		Charset:              "utf8mb4",
		ServerID:             uint32(serverID), // 伪从库 ID（必须唯一，不能与主库/其他从库重复）
		Flavor:               "mariadb",        // 数据库类型（mysql/mariadb）
		ParseTime:            true,
		UseDecimal:           true,
		MaxReconnectAttempts: 100,
		HeartbeatPeriod:      60 * time.Second,
	}
}

func (s *Reader) Prepare() error {
	s.connect()
	return nil
}

func (s *Reader) Start() error {
	s.syncer = replication.NewBinlogSyncer(s.binlogCfg)
	defer s.syncer.Close()
	streamer, err := s.syncer.StartSync(s.getPosition())
	if err != nil {
		return err
	}
	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:

		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		event, err := streamer.GetEvent(ctx)
		cancel()
		if err != nil {
			continue
		}
		if err := s.handle(event); err != nil {
			logs.Error("can not handle event %+v %s", event, err)
			continue
		}
		s.currentPos = s.syncer.GetNextPosition()
	}
}

func (s *Reader) getPosition() mysql.Position {
	pos := s.reloadState()
	if pos.Name == "" {
		c, _ := config.GetConnector(s.conf.Connector)
		conn, _ := common.ConnectMySQL(c)
		{
			var ret []struct {
				File string `gorm:"column:File"`
				Pos  uint32 `gorm:"column:Position"`
			}
			conn.Raw("SHOW BINARY LOG STATUS").Find(&ret)
			if len(ret) > 0 {
				return mysql.Position{
					Name: ret[0].File,
					Pos:  ret[0].Pos,
				}
			}
		}
	}
	s.currentPos = pos
	return pos
}

func (s *Reader) reloadState() mysql.Position {
	var pos mysql.Position
	state := s.opt.StateLoader.Load()
	if state != "" {
		if err := json.Unmarshal([]byte(state), &pos); err != nil {

		}
	}
	return pos
}

func (s *Reader) Stop() error {
	s.cancel()
	return nil
}

func (s *Reader) Save() error {
	j, _ := json.Marshal(s.currentPos)
	return s.opt.StateLoader.Save(string(j))
}
