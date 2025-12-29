package core

import (
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/model"
)

type SchemaOption struct {
	Connector *model.Connector
	Logger    *FileLogger
}

type SchemaManager interface {
	Get(dbname string, tableName string) *schemas.Table
	CreateTable(table *schemas.Table) error
}
