package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"net/http"
	"strings"
)

type writer struct {
	opt    *core.WriterOption
	client *elasticsearch.Client
}

func newWriter(ctx context.Context, opt interface{}) core.Writer {
	return &writer{
		opt: opt.(*core.WriterOption),
	}
}

func (s *writer) Prepare() error {
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
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("elasticsearch returned status code %d", resp.StatusCode)
	}
	s.client = client
	return nil
}

func (s *writer) Execute(e core.Event) error {
	record, err := s.convertObject(e.SourceSchema, e.Record)
	if err != nil {
		return s.opt.Logger.Errorf("can not convert object:%s", err)
	}
	jsonStr, err := json.Marshal(record)
	if err != nil {
		return err
	}
	resp, err := s.client.Index(e.SourceTableName, bytes.NewReader(jsonStr))
	if err != nil {
		return err
	}
	if resp.IsError() {
		return errors.New(resp.String())
	}
	return nil
}

func (s *writer) ExecuteBatch(sourceSchema *schemas.Table, records []core.EventRecord) error {
	var buf bytes.Buffer
	for _, record := range records {
		r, err := s.convertObject(sourceSchema, record)
		if err != nil {
			return s.opt.Logger.Errorf("can not convert object: %s", err)
		}
		delete(r, "_id")
		if err := json.NewEncoder(&buf).Encode(map[string]interface{}{
			"create": map[string]interface{}{
				"_index": sourceSchema.Name,
				"_id":    r["_id"],
			},
		}); err != nil {
			return err
		}
		_ = json.NewEncoder(&buf).Encode(r)
	}
	resp, err := s.client.Bulk(&buf, func(o *esapi.BulkRequest) {
		o.Refresh = "true"
	})
	if err != nil {
		return s.opt.Logger.Errorf("can not execute bulk: %s", err)
	}
	if resp.IsError() {
		return s.opt.Logger.Errorf("can not do bulk to server:%d", resp.StatusCode)
	}
	return nil
}

func (s *writer) convertObject(sourceSchema *schemas.Table, record core.EventRecord) (map[string]interface{}, error) {
	fields := record.Columns
	r := make(map[string]interface{})
	for _, field := range fields {
		vv, err := typMap.Decode(field.Value)
		if err != nil {
			return nil, err
		}
		r[field.Name] = vv
	}
	var pk []string
	for _, col := range sourceSchema.GetPrimaryKeyNames() {
		f, err := record.FieldByName(col)
		if err != nil {
			return nil, err
		}
		vv, _ := typMap.Decode(f.Value)
		pk = append(pk, fmt.Sprint(vv))
	}
	r["_id"] = strings.Join(pk, ":")
	return r, nil
}
