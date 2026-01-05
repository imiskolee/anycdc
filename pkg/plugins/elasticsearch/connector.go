package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
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
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			fmt.Sprintf("http://%s:%d", s.opt.Connector.Host, s.opt.Connector.Port),
		},
		Username:            s.opt.Connector.Username,
		Password:            s.opt.Connector.Password,
		MaxRetries:          10,
		CompressRequestBody: false,
	})
	if err != nil {
		return err
	}
	resp, err := client.Ping()
	if err != nil {
		return err
	}
	if resp.IsError() {
		return errors.New("can not ping server:response status is" + resp.Status())
	}
	_ = client.Close(context.Background())
	return nil
}
