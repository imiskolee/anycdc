package schema

import (
	"github.com/imiskolee/anycdc/pkg/config"
	"time"
)

type CachedSimpleTableSchema struct {
	schema     SimpleTableSchema
	lastSyncAt time.Time
}

type Manager struct {
	connector config.Connector
	tables    map[string]CachedSimpleTableSchema
	resolver  func(connector config.Connector, s, t string) SimpleTableSchema
}

func NewManager(connector string, resolver func(connector config.Connector, s, t string) SimpleTableSchema) *Manager {
	conn, _ := config.GetConnector(connector)
	return &Manager{
		connector: conn,
		resolver:  resolver,
		tables:    make(map[string]CachedSimpleTableSchema)}
}

func (m *Manager) GetTable(schema string, name string) (SimpleTableSchema, error) {
	fullKey := schema + "." + name
	table, ok := m.tables[fullKey]
	if !ok || time.Now().Sub(table.lastSyncAt).Seconds() >= 600 {
		sch := m.resolver(m.connector, schema, name)
		m.tables[fullKey] = CachedSimpleTableSchema{
			schema:     sch,
			lastSyncAt: time.Now(),
		}
	}
	return m.tables[fullKey].schema, nil
}
