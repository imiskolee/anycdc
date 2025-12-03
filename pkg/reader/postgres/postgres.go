package postgres

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/reader"
	"context"
	"encoding/json"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	PostgresExtraPublicationName = "publication_name"
	PostgresExtraSlotName        = "slot_name"
)

func init() {
	reader.Register(config.ConnectorTypePostgres, NewPostgresReader)
}

type PostgresReader struct {
	conf          config.Reader
	ctx           context.Context
	conn          *pgx.Conn
	opt           reader.ReaderOptions
	clientXLogPos pglogrepl.LSN
	relations     map[uint32]pglogrepl.RelationMessageV2
	typeMap       *pgtype.Map
}

func registerTypes(typ *pgtype.Map) {

}

func NewPostgresReader(conf config.Reader, options *reader.ReaderOptions) reader.Reader {
	return &PostgresReader{
		conf:      conf,
		ctx:       context.Background(),
		opt:       *options,
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

func (s *PostgresReader) Save() error {
	j, _ := json.Marshal(s.clientXLogPos)
	return s.opt.StateLoader.Save(string(j))
}
