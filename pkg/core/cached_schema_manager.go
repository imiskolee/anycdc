package core

import (
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"sync"
	"time"
)

type cachedSchema struct {
	schema     schemas.Table
	lastSyncAt time.Time
}

type CachedSchemaManager struct {
	factory SchemaManager
	tables  sync.Map
	mutex   sync.Mutex
}

func NewCachedSchemaManager(factory SchemaManager) SchemaManager {
	return &CachedSchemaManager{
		factory: factory,
	}
}

func (s *CachedSchemaManager) Get(dbName string, tableName string) *schemas.Table {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	now := time.Now()
	key := fmt.Sprintf("%s.%s", dbName, tableName)
	t, ok := s.tables.Load(key)
	if ok {
		tt := t.(cachedSchema)
		if now.Sub(tt.lastSyncAt) < 10*time.Minute {
			return &tt.schema
		}
	}
	schema := s.factory.Get(dbName, tableName)
	if schema != nil {
		s.tables.Store(key, cachedSchema{
			schema:     *schema,
			lastSyncAt: time.Now(),
		})
	}
	return schema
}

func (s *CachedSchemaManager) CreateTable(table *schemas.Table) error {
	return s.factory.CreateTable(table)
}
