package readers

import (
	"bindolabs/anycdc/pkg/config"
	"context"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	PostgresExtraPublicationName = "publication_name"
	PostgresExtraSlotName        = "slot_name"
)

type PostgresReader struct {
	conf          config.Reader
	ctx           context.Context
	conn          *pgx.Conn
	opt           ReaderOptions
	clientXLogPos pglogrepl.LSN
	relations     map[uint32]pglogrepl.RelationMessageV2
	typeMap       *pgtype.Map
}

func NewPostgresReader(conf config.Reader, options ReaderOptions) *PostgresReader {
	return &PostgresReader{
		conf:      conf,
		ctx:       context.Background(),
		opt:       options,
		relations: map[uint32]pglogrepl.RelationMessageV2{},
		typeMap:   pgtype.NewMap(),
	}
}

func (s *PostgresReader) Prepare() error {
	s.connect()
	return s.prepare()
}

func (s *PostgresReader) Start() error {
	return s.start()
}

func (s *PostgresReader) Stop() error {
	return nil
}
