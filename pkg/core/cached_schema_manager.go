package core

import (
	"fmt"
	"time"
)

type cachedSchema struct {
	schema     SimpleTableSchema
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

func (s *CachedSchemaManager) Get(dbName string, tableName string) *SimpleTableSchema {
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

func (s *CachedSchemaManager) CreateTable(database string, table *SimpleTableSchema) error {
	return s.factory.CreateTable(database, table)
}
