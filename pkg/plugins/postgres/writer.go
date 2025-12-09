package postgres

import (
	"context"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"gorm.io/gorm"
)

type Writer struct {
	opt           *core.WriterOption
	connector     *model.Connector
	conn          *gorm.DB
	schemaManager core.SchemaManager
}

func (w *Writer) Prepare() error {
	return w.prepare()
}

func (w *Writer) Execute(e core.Event) error {
	return w.execute(e)
}

func NewWriter(context context.Context, opt interface{}) core.Writer {
	o := opt.(*core.WriterOption)
	return &Writer{
		opt: o,
	}
}
