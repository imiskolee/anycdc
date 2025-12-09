package postgres

import (
	"context"
	"encoding/json"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type Reader struct {
	opt              *core.ReaderOption
	ctx              context.Context
	cancel           context.CancelFunc
	conn             *pgx.Conn
	lastSyncPosition pglogrepl.LSN
	relations        map[uint32]pglogrepl.RelationMessageV2
	typeMap          *pgtype.Map
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
	}
}

func (s *Reader) Prepare() error {
	return s.prepare()
}

func (s *Reader) Start() error {
	return s.start()
}

func (s *Reader) Stop() error {
	return nil
}

func (s *Reader) Position() string {
	j, _ := json.Marshal(s.lastSyncPosition)
	return string(j)
}
