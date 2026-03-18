package mysql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"gorm.io/gorm"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type ssExtra struct {
	FEHost string `json:"fe_host"`
	FEPort int    `json:"fe_port"`
}

type writer struct {
	opt           *core.WriterOption
	conn          *gorm.DB
	schemaManager core.SchemaManager
	Pipeline      *core.Pipeline
	mutex         sync.Mutex
}

var httpClient *http.Client

func init() {
	httpClient = &http.Client{
		Timeout: 30 * time.Second,
	}
}

func NewWriter(ctx context.Context, opt interface{}) core.Writer {
	o := opt.(*core.WriterOption)
	return &writer{
		opt:      o,
		Pipeline: core.NewPipeline(),
		schemaManager: core.NewCachedSchemaManager(NewSchema(context.Background(), &core.SchemaOption{
			Connector: o.Connector,
			Logger:    o.Logger,
		})),
	}
}

func (w *writer) Prepare() error {

	db, err := Connect(w.opt.Connector)
	if err != nil {
		w.opt.Logger.Error("can not prepare connector:%s,%s", w.opt.Connector, err)
		return err
	}
	db.Logger = common_sql.NewLogger(w.opt.Logger)
	w.conn = db

	return nil
}

func (w *writer) Execute(e core.Event) error {
	sch := w.schemaManager.Get(w.opt.Connector.Database, e.DestinationTableName)
	if len(sch.Columns) < 1 {
		w.opt.Logger.Debug("Skipped event, table %s do not exists on the connector", e.DestinationTableName)
		return nil
	}
	if e.Type == core.EventTypeUpdate {
		e.Type = core.EventTypeInsert
	}
	if w.opt.Connector.Type == model.ConnectorTypeStarRocks {
		if e.Type == core.EventTypeDelete {
			return nil
		}
		w.appendBatch(e)
		if time.Now().Sub(w.Pipeline.CreatedAt) > 360*time.Second || w.Pipeline.Count > 100000 {
			return w.processBatch()
		}
		return nil
	}
	e.Record = e.Record.ConvertRecord(sch)
	sqlGenerator := common_sql.NewSQLGenerator(
		w.opt.Connector,
		sch,
		dataTypes,
	)
	sql, params, err := sqlGenerator.DML(e)
	if err != nil {
		return w.opt.Logger.Errorf("cannot generateDML: %v", err)
	}
	err = w.conn.Exec(sql, params...).Error
	if err != nil {
		return w.opt.Logger.Errorf("cannot execute: %v", err)
	}
	return nil
}

func (w *writer) ExecuteBatch(sourceSchema *schemas.Table, records []core.Event) error {
	w.opt.Logger.Error("Starting Batch %s", sourceSchema.Name)
	tableName := records[0].DestinationTableName
	sch := w.schemaManager.Get(w.opt.Connector.Database, tableName)
	if len(sch.Columns) < 1 {
		w.opt.Logger.Debug("Skipped event, table %s do not exists on the connector", tableName)
		return nil
	}
	convertedRecord := make([]core.EventRecord, len(records))
	for i, record := range records {
		convertedRecord[i] = record.Record.ConvertRecord(sch)
	}

	if w.opt.Connector.Type == model.ConnectorTypeStarRocks {
		return w.pushStarRocks(sch, convertedRecord)
	}

	sql, params, err := batchUpsert(w.opt.Connector, sch, dataTypes, convertedRecord)
	if err != nil {
		return w.opt.Logger.Errorf("cannot generate batch SQL: %v", err)
	}
	err = w.conn.Exec(sql, params...).Error
	if err != nil {
		return w.opt.Logger.Errorf("cannot execute: %v, sql=%s,vals=%+v", err, sql, params)
	}
	w.opt.Logger.Debug("Successfully executed batch SQL,records = %d", len(convertedRecord))
	return nil
}

func (w *writer) processBatch() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	pipeline := w.Pipeline
	w.Pipeline = core.NewPipeline()
	for table, batch := range pipeline.Events {
		w.opt.Logger.Error("Starting processBatch:%s %d", table, len(batch))
		if err := w.ExecuteBatch(&batch[0].SourceSchema, batch); err != nil {
			return err
		}
	}

	return nil
}

func (w *writer) pushStarRocks(sch *schemas.Table, events []core.EventRecord) error {
	w.opt.Logger.Error("Starting Push To SR, table name=%s", sch.Name)
	var records []string
	var jsonPaths []string
	var columns []string
	for _, col := range events[0].Columns {
		jsonPaths = append(jsonPaths, fmt.Sprintf("\"$.`%s`\"", col.Name))
		columns = append(columns, fmt.Sprintf("`%s`", col.Name))
	}

	for _, event := range events {
		data := make(map[string]interface{})
		for _, col := range sch.Columns {
			var val interface{}
			f, err := event.FieldByName(col.Name)
			if err == nil {
				val, err = dataTypes.Decode(f.Value)
			}
			if val == nil {
				if !col.Nullable {
					switch col.DataType {
					case schemas.TypeBool:
						val = false
					case schemas.TypeString:
						val = ""
					case schemas.TypeUint, schemas.TypeInt, schemas.TypeDecimal:
						val = 0
					case schemas.TypeJSON:
						val = "{}"
					case schemas.TypeTimestamp:
						val = time.Time{}
					default:
						val = ""
					}
				}
			}
			data[fmt.Sprintf("`%s`", col.Name)] = val
		}
		jsonStr, _ := json.Marshal(data)
		records = append(records, string(jsonStr))
	}
	converted := strings.Join(records, "\n")

	var respData struct {
		Status  string `json:"Status"`
		Message string `json:"Message"`
	}

	var ssE ssExtra
	if err := json.Unmarshal([]byte(w.opt.Connector.Extra), &ssE); err != nil {
		w.opt.Logger.Error("Unmarshal err: %v", err)
		return err
	}
	url := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load",
		ssE.FEHost,
		ssE.FEPort,
		w.opt.Connector.Database,
		sch.Name,
	)

	request, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(converted)))
	request.SetBasicAuth(w.opt.Connector.Username, w.opt.Connector.Password)
	request.Header.Set("format", "json")
	request.Header.Set("jsonpaths", "["+strings.Join(jsonPaths, ",")+"]")
	request.Header.Set("columns", strings.Join(columns, ","))
	request.Header.Set("strict_mode", "true")
	request.Header.Set("Expect", "100-continue")
	request.Header.Set("ignore_json_size", "true")

	resp, err := httpClient.Do(request)
	if err != nil {
		w.opt.Logger.Error("Can not push to sr:%s", err)
		return err
	}
	content, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err := json.Unmarshal(content, &respData); err != nil {
		w.opt.Logger.Error("Can not push to sr:%s", err)
	}
	if resp.StatusCode != 200 || respData.Status != "Success" {
		return w.opt.Logger.Errorf("Can not load data:%s", string(content))
	}
	w.opt.Logger.Error("Completed push to SR %s", sch.Name)
	return nil
}

func (w *writer) appendBatch(event core.Event) {
	w.Pipeline.Append("", event)
}
