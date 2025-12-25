package postgres

import (
	"context"
	"github.com/imiskolee/anycdc/pkg/core"
)

type connector struct {
	opt *core.ConnectorOption
}

func newConnector(ctx context.Context, opt interface{}) core.Connector {
	return &connector{
		opt: opt.(*core.ConnectorOption),
	}
}

func (s *connector) Test() error {
	_, err := Connect(s.opt.Connector)
	return err
}
