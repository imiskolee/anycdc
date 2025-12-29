package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/core/types"
	"github.com/imiskolee/anycdc/pkg/model"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"time"
)

type dumper struct {
	ctx           context.Context
	cancel        context.CancelFunc
	opt           *core.DumperOption
	conn          *pgxpool.Pool
	schemaManager core.SchemaManager
	wg            sync.WaitGroup
	stopped       bool
}

func newDumper(ctx context.Context, opts interface{}) core.Dumper {
	c, cancel := context.WithCancel(ctx)
	dumper := &dumper{
		ctx:    c,
		cancel: cancel,
		opt:    opts.(*core.DumperOption),
	}
	dumper.schemaManager = core.NewCachedSchemaManager(newSchema(ctx, &core.SchemaOption{
		Connector: dumper.opt.Connector,
	}))
	return dumper
}

func (d *dumper) Prepare() error {
	conn, err := connectPGX(d.opt.Connector)
	if err != nil {
		return d.opt.Logger.Errorf("can not connector to %s, %s", d.opt.Connector.Name, err)
	}
	d.conn = conn
	return nil
}

func (d *dumper) Stop() error {
	d.stopped = true
	if d.conn != nil {
		d.conn.Close()
	}
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
		var record core.EventRecord
		if err := json.Unmarshal([]byte(table.LastDumperKey), &record); err != nil {
			return err
		}
		lastRecord = &record
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
			if err := d.opt.Subscriber.DumperEvent(sch, batch); err != nil {
				return d.opt.Logger.Errorf("dump failed,can not handle batch %v", err)
			}
			if lastRecord == nil {
				total += len(batch)
			} else {
				total += len(batch) - 1
			}
		}
		if now.Sub(lastLogAt) > 30*time.Second {
			lastLogAt = now
			d.opt.Logger.Info("successful dump table %s, records %d", table, total)
		}
		if len(batch) > 0 {
			lastRecord = &batch[len(batch)-1]
		}
		if len(batch) < batchSize {
			d.opt.Logger.Info("successful dump records %d on table %s,", total, table)
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
	rows, err := d.conn.Query(d.ctx, sql, vals...)
	if err != nil {
		return nil, d.opt.Logger.Errorf("failed run batch on table %s,%s", sch.Name, err)
	}
	defer rows.Close()
	return rowToMap(rows)
}

func rowToMap(rows pgx.Rows) ([]core.EventRecord, error) {
	cols := make([]string, len(rows.FieldDescriptions()))
	columnTypes := make([]uint32, len(rows.FieldDescriptions()))
	for i, fd := range rows.FieldDescriptions() {
		cols[i] = string(fd.Name)
		columnTypes[i] = fd.DataTypeOID
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns found")
	}
	var ret []core.EventRecord
	for rows.Next() {
		rawValues := rows.RawValues()
		var record core.EventRecord
		for i, data := range rawValues {
			column, err := convertFromPGX(columnTypes[i], data)
			if err != nil {
				return nil, err
			}
			record.Set(cols[i], column)
		}
		ret = append(ret, record)
	}
	return ret, nil
}
