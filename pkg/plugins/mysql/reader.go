package mysql

import (
	"context"
	"encoding/json"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
)

type Reader struct {
	opt           *core.ReaderOption
	binlogCfg     replication.BinlogSyncerConfig
	ctx           context.Context
	syncer        *replication.BinlogSyncer
	currentPos    mysql.Position
	cancel        context.CancelFunc
	connector     *model.Connector
	schemaManager core.SchemaManager
	done          chan bool
}

func NewReader(ctx context.Context, opt interface{}) core.Reader {
	o := opt.(*core.ReaderOption)
	ctx, cancel := context.WithCancel(ctx)
	return &Reader{
		ctx:    ctx,
		cancel: cancel,
		opt:    o,
		schemaManager: NewSchema(ctx, &core.SchemaOption{
			Connector: o.Connector,
			Logger:    o.Logger,
		}),
	}
}

func (r *Reader) Prepare() error {
	return r.prepare()
}

func (r *Reader) Start() error {
	r.start()
	return nil
}

func (r *Reader) Stop() error {
	return r.stop()
}

func (r *Reader) Position() string {
	j, _ := json.Marshal(r.currentPos)
	return string(j)
}
