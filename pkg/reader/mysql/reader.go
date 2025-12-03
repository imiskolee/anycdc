package mysql

import (
	"bindolabs/anycdc/pkg/common_mysql"
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/reader"
	"bindolabs/anycdc/pkg/schema"
	"context"
	"encoding/json"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"log"
	"strconv"
	"time"
)

const (
	extraParamServerID string = "server_id"
)

func init() {
	reader.Register(config.ConnectorTypeMySQL, NewMySQLReader)
}

type MySQLReader struct {
	conf       config.Reader
	schema     *schema.Manager
	opt        reader.ReaderOptions
	binlogCfg  replication.BinlogSyncerConfig
	ctx        context.Context
	syncer     *replication.BinlogSyncer
	currentPos mysql.Position
}

func NewMySQLReader(conf config.Reader, opt *reader.ReaderOptions) reader.Reader {

	return &MySQLReader{
		conf:   conf,
		opt:    *opt,
		ctx:    context.Background(),
		schema: schema.NewManager(conf.Connector, common_mysql.SyncSchema),
	}
}

func (s *MySQLReader) connect() {
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

func (s *MySQLReader) Prepare() error {
	s.connect()
	return nil
}

func (s *MySQLReader) Start() error {
	s.syncer = replication.NewBinlogSyncer(s.binlogCfg)
	defer s.syncer.Close()
	streamer, err := s.syncer.StartSync(s.getPosition())
	if err != nil {
		return err
	}

	for {
		ctx, cancel := context.WithTimeout(s.ctx, 1*time.Second)
		event, err := streamer.GetEvent(ctx)
		cancel()
		if err != nil {
			continue
		}
		if err := s.handle(event); err != nil {
			log.Println("Sync error:" + err.Error())
			continue
		}
		s.currentPos = s.syncer.GetNextPosition()
	}
}

func (s *MySQLReader) getPosition() mysql.Position {
	pos := s.reloadState()
	if pos.Name == "" {
		c, _ := config.GetConnector(s.conf.Connector)
		conn, _ := common_mysql.Connect(c)
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
	return pos
}

func (s *MySQLReader) reloadState() mysql.Position {
	var pos mysql.Position
	state := s.opt.StateLoader.Load()
	if state != "" {
		if err := json.Unmarshal([]byte(state), &pos); err != nil {
			panic("Can not load state:" + err.Error())
		}
	}
	return pos
}

func (s *MySQLReader) Stop() error {
	return nil
}

func (s *MySQLReader) Save() error {
	j, _ := json.Marshal(s.currentPos)
	return s.opt.StateLoader.Save(string(j))
}
