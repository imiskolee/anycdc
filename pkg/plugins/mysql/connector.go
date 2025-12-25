package mysql

import (
	"context"
	"github.com/imiskolee/anycdc/pkg/core"
)

type connector struct {
	opt *core.ConnectorOption
}

func NewConnector(ctx context.Context, opt interface{}) core.Connector {
	o := opt.(*core.ConnectorOption)
	return &connector{
		opt: o,
	}
}

func (s *connector) Test() error {
	_, err := Connect(s.opt.Connector)
	if err != nil {
		return err
	}
	return nil
}
