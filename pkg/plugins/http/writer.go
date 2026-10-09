package http

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/core/types"
)

const (
	defaultBatchSize           = 100
	defaultMaxBatchTimeSeconds = 60
	// maxEventsPerRequest 单次请求最多事件数，超出分片发送（对接方限制）。
	maxEventsPerRequest = 100
)

// httpExtra 是 HTTP writer 的扩展参数，存放于 connector 的 Extra 字段(JSON)。
type httpExtra struct {
	URL                 string            `json:"url"`
	BatchSize           int               `json:"batch_size"`
	MaxBatchTimeSeconds int               `json:"max_batch_time_seconds"`
	Headers             map[string]string `json:"headers"`
	// SigningSecret 为对接方提供的 whsec_... 签名密钥。配置后 writer 会为每个
	// 请求计算 X-Wonder-Signature (HMAC-SHA256 of "t.body") 并随请求头发送。
	SigningSecret string `json:"signing_secret"`
}

type writer struct {
	opt    *core.WriterOption
	client *http.Client
	extra  httpExtra
	dec    *types.Map

	mu       sync.Mutex
	buffer   []core.Event
	lastSend time.Time
}

// newWriter 是插件工厂函数，实现 core.Plugin.WriterFactory。
func newWriter(ctx context.Context, opt interface{}) core.Writer {
	return &writer{
		opt: opt.(*core.WriterOption),
		dec: types.NewDefaultTypeMap(),
	}
}

func (w *writer) Prepare() error {
	opt := w.opt
	extra := httpExtra{}
	if opt.Connector.Extra != "" {
		if err := json.Unmarshal([]byte(opt.Connector.Extra), &extra); err != nil {
			return opt.Logger.Errorf("cannot parse http writer extra: %v", err)
		}
	}
	if extra.URL == "" {
		return opt.Logger.Errorf("http writer requires extra.url")
	}
	if extra.BatchSize == 0 {
		extra.BatchSize = defaultBatchSize
	}
	if extra.MaxBatchTimeSeconds == 0 {
		extra.MaxBatchTimeSeconds = defaultMaxBatchTimeSeconds
	}
	w.extra = extra

	w.client = &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		},
	}
	w.mu.Lock()
	w.lastSend = time.Now()
	w.mu.Unlock()

	go func() {
		ticker := time.NewTicker(time.Duration(w.extra.MaxBatchTimeSeconds) * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			w.mu.Lock()
			batch := w.buffer
			need := len(batch) > 0
			if need {
				w.buffer = nil
				w.lastSend = time.Now()
			}
			w.mu.Unlock()
			if !need {
				continue
			}
			if err := w.post(batch); err != nil {
				opt.Logger.Errorf("http writer ticker flush failed: %v, events=%d", err, len(batch))
			}
		}
	}()

	return nil
}

func (w *writer) Execute(e core.Event) error {
	// delete 事件跳过，不同步
	if e.Type == core.EventTypeDelete {
		return nil
	}

	w.mu.Lock()
	w.buffer = append(w.buffer, e)
	n := len(w.buffer)
	timeoutReached := n > 0 && time.Since(w.lastSend) >= time.Duration(w.extra.MaxBatchTimeSeconds)*time.Second
	var batch []core.Event
	if n >= w.extra.BatchSize || timeoutReached {
		batch = w.buffer
		w.buffer = nil
		w.lastSend = time.Now()
	}
	w.mu.Unlock()

	if batch != nil {
		if err := w.post(batch); err != nil {
			w.opt.Logger.Errorf("http writer flush failed: %v, events=%d", err, len(batch))
		}
	}
	return nil
}

func (w *writer) ExecuteBatch(sourceSchema *schemas.Table, records []core.Event) error {
	if len(records) == 0 {
		return nil
	}
	return w.post(records)
}

// outEvent 是发送给远端单条事件的约定结构。
type outEvent struct {
	EventType    string                 `json:"event_type"`
	EventID      string                 `json:"event_id"`
	Record       map[string]interface{} `json:"record"`
	ChangedFields []string              `json:"changed_fields"`
}

type payload struct {
	Events []outEvent `json:"events"`
}

func (w *writer) opString(t core.EventType) string {
	switch t {
	case core.EventTypeUpdate:
		return "update"
	case core.EventTypeInsert:
		return "insert"
	default:
		return "unknown"
	}
}

// recordToMap 将事件记录解码为可 JSON 化的 map。
func (w *writer) recordToMap(rec core.EventRecord) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(rec.Columns))
	for _, field := range rec.Columns {
		v, err := w.dec.Decode(field.Value)
		if err != nil {
			return nil, fmt.Errorf("decode field %s: %v", field.Name, err)
		}
		out[field.Name] = v
	}
	return out, nil
}

// changedFields 计算本次更新的实际变更列；insert 返回全部列。
func (w *writer) changedFields(ev core.Event) ([]string, error) {
	if ev.Type == core.EventTypeInsert || ev.OldRecord == nil {
		fields := make([]string, 0, len(ev.Record.Columns))
		for _, f := range ev.Record.Columns {
			fields = append(fields, f.Name)
		}
		return fields, nil
	}
	var changed []string = make([]string, 0, len(ev.Record.Columns))
	for _, f := range ev.Record.Columns {
		old, err := ev.OldRecord.FieldByName(f.Name)
		if err != nil {
			// 新记录有此列而旧记录没有，视为变更
			changed = append(changed, f.Name)
			continue
		}
		newVal, err := w.dec.Decode(f.Value)
		if err != nil {
			return nil, err
		}
		oldVal, err := w.dec.Decode(old.Value)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(newVal, oldVal) {
			changed = append(changed, f.Name)
		}
	}
	return changed, nil
}

func (w *writer) post(records []core.Event) error {
	events := make([]outEvent, 0, len(records))
	for _, ev := range records {
		if ev.Type == core.EventTypeDelete {
			continue
		}
		record, err := w.recordToMap(ev.Record)
		if err != nil {
			return w.opt.Logger.Errorf("http writer serialize record failed: %v", err)
		}
		fields, err := w.changedFields(ev)
		if err != nil {
			return w.opt.Logger.Errorf("http writer compute changed_fields failed: %v", err)
		}
		events = append(events, outEvent{
			EventType:    fmt.Sprintf("%s.%s", ev.SourceSchema.Name, w.opString(ev.Type)),
			EventID:      w.eventID(ev, record),
			Record:       record,
			ChangedFields: fields,
		})
	}
	if len(events) == 0 {
		return nil
	}
	// 分批：单次请求最多 maxEventsPerRequest 条，超出分片发送。
	for i := 0; i < len(events); i += maxEventsPerRequest {
		end := i + maxEventsPerRequest
		if end > len(events) {
			end = len(events)
		}
		if err := w.sendEvents(events[i:end]); err != nil {
			return err
		}
	}
	return nil
}

// eventID 由「表名.主键值.binlog位移」派生，满足：同一条数据库变更在被重试/重启后
// 仍产生相同 ID，从而对接方的 event_id 去重不重复发信；ID 仅含字母数字 .-_。
func (w *writer) eventID(ev core.Event, record map[string]interface{}) string {
	pos := sanitizeID(ev.LastPOS)
	if pos == "" {
		pos = "0"
	}
	table := sanitizeID(ev.SourceSchema.Name)
	if pks := ev.SourceSchema.GetPrimaryKeyNames(); len(pks) > 0 {
		parts := make([]string, 0, len(pks))
		for _, name := range pks {
			if v, ok := record[name]; ok && v != nil {
				parts = append(parts, sanitizeID(fmt.Sprintf("%v", v)))
			}
		}
		if len(parts) > 0 {
			return fmt.Sprintf("%s.%s.%s", table, strings.Join(parts, "_"), pos)
		}
	}
	// 无主键或主键为空：用 record+pos 的 sha256 短哈希兜底，同样在同批重试下稳定。
	h := sha256.Sum256([]byte(fmt.Sprintf("%v:%s", record, pos)))
	return fmt.Sprintf("%s.%x.%s", table, h[:8], pos)
}

// sanitizeID 把任意字符串规整为 event_id 允许的字符集(字母数字 . _ -)。
func sanitizeID(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			return r
		default:
			return '_'
		}
	}, s)
}

// sendEvents 序列化一个分片并发送；超时/5xx/429 时指数退避重试，其余错误直接返回。
func (w *writer) sendEvents(events []outEvent) error {
	body, err := json.Marshal(payload{Events: events})
	if err != nil {
		return w.opt.Logger.Errorf("http writer marshal payload failed: %v", err)
	}
	delays := []time.Duration{200 * time.Millisecond, 500 * time.Millisecond, 1500 * time.Millisecond}
	var lastErr error
	for i := 0; i <= len(delays); i++ {
		lastErr = w.sendOnce(body)
		if lastErr == nil {
			w.opt.Logger.Debug("http writer flushed %d events to %s", len(events), w.extra.URL)
			return nil
		}
		if !w.isRetryable(lastErr) || i == len(delays) {
			break
		}
		time.Sleep(delays[i])
	}
	return lastErr
}

func (w *writer) sendOnce(body []byte) error {
	req, err := http.NewRequest(http.MethodPost, w.extra.URL, bytes.NewReader(body))
	if err != nil {
		return w.opt.Logger.Errorf("http writer build request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	for k, v := range w.extra.Headers {
		req.Header.Set(k, v)
	}
	if w.extra.SigningSecret != "" {
		// 签名原文为 "t.body"，t 为当前 Unix 秒；密钥为对接方提供的 whsec_ 原样。
		ts := time.Now().Unix()
		sig := w.hmacSHA256(fmt.Sprintf("%d.%s", ts, body))
		req.Header.Set("X-Wonder-Signature", fmt.Sprintf("t=%d,v1=%s", ts, sig))
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &statusError{code: resp.StatusCode}
	}
	return nil
}

func (w *writer) hmacSHA256(message string) string {
	mac := hmac.New(sha256.New, []byte(w.extra.SigningSecret))
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

type statusError struct{ code int }

func (e *statusError) Error() string { return fmt.Sprintf("http writer got status %d", e.code) }

// isRetryable 判断错误是否值得重试：429/5xx 与网络类错误(超时/连接重置)可重试，鉴权等业务失败不可。
func (w *writer) isRetryable(err error) bool {
	var se *statusError
	if errors.As(err, &se) {
		return se.code == http.StatusTooManyRequests || se.code >= 500
	}
	var ue *url.Error
	return errors.As(err, &ue)
}