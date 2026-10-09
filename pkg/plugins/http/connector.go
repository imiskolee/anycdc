package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

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

// Test 校验 HTTP 连接器配置：extra.url 必须为合法的 http/https 地址。
// 不做远端连通性探测，避免对任意目标地址发起真实请求。
func (s *connector) Test() error {
	c := s.opt.Connector
	var extra httpExtra
	if c.Extra != "" {
		if err := json.Unmarshal([]byte(c.Extra), &extra); err != nil {
			return fmt.Errorf("cannot parse http connector extra: %v", err)
		}
	}
	if extra.URL == "" {
		return fmt.Errorf("http connector requires extra.url")
	}
	u, err := url.Parse(extra.URL)
	if err != nil {
		return fmt.Errorf("http connector invalid url: %v", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("http connector url scheme must be http or https, got %q", u.Scheme)
	}
	return nil
}