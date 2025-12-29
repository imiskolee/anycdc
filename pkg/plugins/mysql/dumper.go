package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/core/types"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"gorm.io/gorm"
	"strings"
	"time"
)

type dumper struct {
	opt           *core.DumperOption
	conn          *gorm.DB
	stopped       bool
	schemaManager core.SchemaManager
}

func NewDumper(ctx context.Context, opt interface{}) core.Dumper {
	o := opt.(*core.DumperOption)
	return &dumper{
		opt: o,
		schemaManager: core.NewCachedSchemaManager(NewSchema(context.Background(), &core.SchemaOption{
			Connector: o.Connector,
		})),
	}
}

func (d *dumper) Prepare() error {
	conn, err := Connect(d.opt.Connector)
	if err != nil {
		return err
	}
	d.conn = conn
	return nil
}

func (d *dumper) StartDumpTable(table *model.TaskTable) error {
	sch := d.schemaManager.Get(d.opt.Connector.Database, table.Table)
	if sch == nil {
		return d.opt.Logger.Errorf("start dumper failed,can not find schema for table %s", table.Table)
	}
	primaryKeys := sch.GetPrimaryKeys()
	if len(primaryKeys) == 0 {
		return d.opt.Logger.Errorf("start dumper failed,can not find primary keys for table %s", table.Table)
	}
	var lastRecord *core.EventRecord
	if table.LastDumperKey != "" {
		record := make(map[string]interface{})
		if err := json.Unmarshal([]byte(table.LastDumperKey), &record); err != nil {
			return err
		}
		lastRecord = &core.EventRecord{}
		for k, v := range record {
			f, ok := sch.GetFieldByName(k)
			if !ok {
				continue
			}
			lastRecord.Set(k, types.NewTypedData(f.DataType, v))
		}
	}
	total := 0
	batchSize := d.opt.BatchSize
	var lastLogAt time.Time
	for {
		if d.stopped {
			d.opt.Logger.Info("stop dump, because of received stop signal")
			return nil
		}
		now := time.Now()
		batch, err := d.queryBatch(sch, batchSize, lastRecord)
		if err != nil {
			return d.opt.Logger.Errorf("dump failed,can not query batch %v", err)
		}
		if len(batch) > 0 {
			if lastRecord != nil {
				batch = batch[1:]
			}
			if err := d.opt.Subscriber.DumperEvent(sch, batch); err != nil {
				return d.opt.Logger.Errorf("dump failed,can not handle batch %v", err)
			}
			total = len(batch)
		}
		if now.Sub(lastLogAt) > 30*time.Second {
			lastLogAt = now
			d.opt.Logger.Info("successful dump table %s, records %d", table.Table, total)
		}
		if len(batch) > 0 {
			lastRecord = &batch[len(batch)-1]
		}
		if len(batch) < (batchSize - 1) {
			d.opt.Logger.Info("successful dump records %d on table %s,", total, table.Table)
			break
		}
	}
	return nil
}

func (d *dumper) queryBatch(sch *schemas.Table, batchSize int, lastRecord *core.EventRecord) ([]core.EventRecord, error) {
	generator := common_sql.NewSQLGenerator(d.opt.Connector, sch, types.NewDefaultTypeMap())
	sql, vals, err := generator.Dumper(batchSize, lastRecord)
	if err != nil {
		return nil, err
	}
	rawDB, err := d.conn.DB()
	if err != nil {
		return nil, err
	}

	rows, err := rawDB.Query(sql, vals...)
	if err != nil {
		return nil, d.opt.Logger.Errorf("failed run batch on table %s,%s", sch.Name, err)
	}
	defer rows.Close()
	return rowToMap(rows)
}

func (d *dumper) Stop() error {
	d.stopped = true
	return nil
}

func rowToMap(rows *sql.Rows) ([]core.EventRecord, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found")
	}
	var ret []core.EventRecord
	for rows.Next() {
		var record core.EventRecord
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(values))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, errors.New("can not scan column: " + err.Error())
		}

		for k, column := range columns {
			colType := columnTypes[k]
			dest := values[k]
			typ, _ := getBuiltType(strings.ToLower(colType.DatabaseTypeName()))
			td, err := dataTypes.Encode(typ, dest)
			if err != nil {
				return nil, err
			}
			record.Set(column, td)
		}
		ret = append(ret, record)
	}
	return ret, nil
}
