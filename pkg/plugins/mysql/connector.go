package mysql

import (
	"context"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
)

type Connector struct {
	conn *model.Connector
}

func NewConnector(ctx context.Context, opt interface{}) core.Connector {
	o := opt.(*model.Connector)
	return &Connector{
		conn: o,
	}
}

func (s *Connector) Test() error {
	_, err := Connect(s.conn)
	if err != nil {
		return err
	}
	return nil
}
