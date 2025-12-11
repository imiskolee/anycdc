package postgres

import (
	"context"
	"encoding/json"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"time"
)

type Reader struct {
	opt              *core.ReaderOption
	ctx              context.Context
	cancel           context.CancelFunc
	conn             *pgx.Conn
	lastSyncPosition pglogrepl.LSN
	relations        map[uint32]pglogrepl.RelationMessageV2
	typeMap          *pgtype.Map
	running          bool
	done             chan bool
	schema           core.SchemaManager
	lastEventAt      time.Time
}

func (s *Reader) LastEventAt() time.Time {
	return s.lastEventAt
}

func NewReader(ctx context.Context, opt interface{}) core.Reader {
	c, cancel := context.WithCancel(ctx)
	o := opt.(*core.ReaderOption)
	return &Reader{
		ctx:       c,
		cancel:    cancel,
		opt:       o,
		relations: make(map[uint32]pglogrepl.RelationMessageV2),
		typeMap:   pgtype.NewMap(),
		done:      make(chan bool),
		schema: core.NewCachedSchemaManager(NewSchema(ctx, &core.SchemaOption{
			Connector: o.Connector,
		})),
	}
}

func (s *Reader) Prepare() error {
	return s.prepare()
}

func (s *Reader) Start() error {
	return s.start()
}

func (s *Reader) Stop() error {
	if !s.running {
		return nil
	}
	s.cancel()
	res := <-s.done
	if res {
		return nil
	}
	return s.opt.Logger.Errorf("failed stop reader")
}

func (s *Reader) Position() string {
	j, _ := json.Marshal(s.lastSyncPosition)
	return string(j)
}
