package core

import (
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"time"
)

type cachedSchema struct {
	schema     schemas.Table
	lastSyncAt time.Time
}

type CachedSchemaManager struct {
	factory SchemaManager
	tables  map[string]cachedSchema
}

func NewCachedSchemaManager(factory SchemaManager) SchemaManager {
	return &CachedSchemaManager{
		factory: factory,
		tables:  make(map[string]cachedSchema),
	}
}

func (s *CachedSchemaManager) Get(dbName string, tableName string) *schemas.Table {
	now := time.Now()
	key := fmt.Sprintf("%s.%s", dbName, tableName)
	if t, ok := s.tables[key]; ok && now.Sub(t.lastSyncAt) < 1*time.Minute {
		return &t.schema
	}
	schema := s.factory.Get(dbName, tableName)
	if schema != nil {
		s.tables[key] = cachedSchema{
			schema:     *schema,
			lastSyncAt: time.Now(),
		}
	}
	return schema
}

func (s *CachedSchemaManager) CreateTable(table *schemas.Table) error {
	return s.factory.CreateTable(table)
}
