package mysql

import (
	"context"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"gorm.io/gorm"
)

type Writer struct {
	ctx           context.Context
	opt           *core.WriterOption
	conn          *gorm.DB
	schemaManager core.SchemaManager
	connector     *model.Connector
}

func NewWriter(ctx context.Context, opt interface{}) core.Writer {
	o := opt.(*core.WriterOption)
	return &Writer{
		ctx: ctx,
		opt: o,
		schemaManager: NewSchema(ctx, &core.SchemaOption{
			Connector: o.Connector,
			Logger:    o.Logger,
		}),
	}
}

func (w *Writer) Prepare() error {
	return w.prepare()
}

func (w *Writer) Execute(e core.Event) error {
	return w.execute(e)
}
