package postgres

import (
	"context"
	"encoding/json"
	"github.com/imiskolee/anycdc/pkg/config"
	"github.com/imiskolee/anycdc/pkg/reader"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	ExtraPublicationName = "publication_name"
	ExtraSlotName        = "slot_name"
)

func init() {
	reader.Register(config.ConnectorTypePostgres, NewReader)
}

type Reader struct {
	conf          config.Reader
	ctx           context.Context
	cancel        context.CancelFunc
	conn          *pgx.Conn
	opt           reader.ReaderOptions
	clientXLogPos pglogrepl.LSN
	relations     map[uint32]pglogrepl.RelationMessageV2
	typeMap       *pgtype.Map
}

func NewReader(conf config.Reader, options *reader.ReaderOptions) reader.Reader {
	ctx, cancel := context.WithCancel(context.TODO())

	return &Reader{
		conf:      conf,
		ctx:       ctx,
		cancel:    cancel,
		opt:       *options,
		relations: map[uint32]pglogrepl.RelationMessageV2{},
		typeMap:   pgtype.NewMap(),
	}
}

func (s *Reader) Prepare() error {
	s.connect()
	return s.prepare()
}

func (s *Reader) Start() error {
	return s.start()
}

func (s *Reader) Stop() error {
	s.cancel()
	return nil
}

func (s *Reader) Save() error {
	j, _ := json.Marshal(s.clientXLogPos)
	return s.opt.StateLoader.Save(string(j))
}
