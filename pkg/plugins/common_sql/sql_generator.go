package common_sql

import (
	"errors"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/core/types"
	"github.com/imiskolee/anycdc/pkg/model"
	"strings"
)

var sqlQuotes = map[string]string{
	model.ConnectorTypeMySQL:     "`",
	model.ConnectorTypePostgres:  `"`,
	model.ConnectorTypeStarRocks: "`",
}

type SQLGenerator struct {
	connector *model.Connector
	schema    *schemas.Table
	typeMap   *types.Map
}

func NewSQLGenerator(connector *model.Connector, schema *schemas.Table, typeMap *types.Map) *SQLGenerator {
	return &SQLGenerator{
		connector: connector,
		schema:    schema,
		typeMap:   typeMap,
	}
}

func (s *SQLGenerator) DML(e core.Event) (string, []interface{}, error) {
	switch e.Type {
	case core.EventTypeInsert:
		return s.toInsert(e)
	case core.EventTypeUpdate:
		return s.toUpdate(e)
	case core.EventTypeDelete:
		return s.toDelete(e)
	}
	return "", nil, errors.New("invalid event type")
}

func (s *SQLGenerator) Dumper(batchSize int, lastRecord *core.EventRecord) (string, []interface{}, error) {
	var whereClauses []string
	var whereValues []interface{}
	var orderByClauses []string
	primaryKeys := s.schema.GetPrimaryKeyNames()

	for _, pk := range primaryKeys {
		orderByClauses = append(orderByClauses, fmt.Sprintf("%s ASC", s.quote(pk)))
	}
	if lastRecord != nil {
		quote := "?"
		for idx, pk := range primaryKeys {
			if s.connector.Type == model.ConnectorTypePostgres {
				quote = fmt.Sprintf("$%d", idx+1)
			}
			fieldValue, err := lastRecord.FieldByName(pk)
			if err != nil {
				return "", nil, err
			}
			v, err := s.typeMap.Decode(fieldValue.Value)
			if err != nil {
				return "", nil, err
			}
			whereClauses = append(whereClauses, fmt.Sprintf("%s >= %s", s.quote(pk), quote))
			whereValues = append(whereValues, v)
		}
	} else {
		whereClauses = []string{"1 = 1"}
		whereValues = nil

	}

	sql := fmt.Sprintf(`SELECT * FROM %s WHERE %s ORDER BY %s LIMIT %d`,
		s.quote(s.schema.Name),
		strings.Join(whereClauses, " AND "),
		strings.Join(orderByClauses, ", "),
		batchSize,
	)
	return sql, whereValues, nil
}

func (s *SQLGenerator) toInsert(e core.Event) (string, []interface{}, error) {
	primaryKeys := s.schema.GetPrimaryKeyNames()
	columns := make([]string, 0, len(e.Record.Columns))
	values := make([]interface{}, 0, len(e.Record.Columns))
	updateClause := make([]string, 0, len(e.Record.Columns)-len(primaryKeys))
	updateValues := make([]interface{}, 0, len(e.Record.Columns)-len(primaryKeys))

	for _, val := range e.Record.Columns {
		columns = append(columns, s.quote(val.Name))
		v, err := s.typeMap.Decode(val.Value)
		if err != nil {
			return "", nil, errors.New(fmt.Sprintf("field %s decode fail,%s", val.Name, err.Error()))
		}
		values = append(values, v)

		isPK := false
		for _, pk := range primaryKeys {
			if pk == val.Name {
				isPK = true
				break
			}
		}
		if !isPK {
			updateClause = append(updateClause, fmt.Sprintf("%s = ?", s.quote(val.Name)))
			updateValues = append(updateValues, v)
		}
	}

	quotes := make([]string, 0, len(columns))
	for i := 0; i < len(columns); i++ {
		quotes = append(quotes, "?")
	}

	insertSQL := ""

	switch s.connector.Type {
	case model.ConnectorTypeMySQL:
		insertSQL = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s ",
			s.quote(s.schema.Name),
			strings.Join(columns, ", "),
			strings.Join(quotes, ", "),
			strings.Join(updateClause, ", "),
		)
		values = append(values, updateValues...)
		break
	case model.ConnectorTypePostgres:
		pks := make([]string, 0, len(primaryKeys))
		for _, pk := range primaryKeys {
			pks = append(pks, s.quote(pk))
		}
		insertSQL = fmt.Sprintf(
			`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s`,
			s.quote(s.schema.Name),
			strings.Join(columns, ", "),
			strings.Join(quotes, ", "),
			strings.Join(pks, ", "),
			strings.Join(updateClause, ", "),
		)
		values = append(values, updateValues...)
		break
	case model.ConnectorTypeStarRocks:
		insertSQL = fmt.Sprintf(
			"INSERT OVERWRITE %s (%s) VALUES (%s)",
			s.quote(s.schema.Name),
			strings.Join(columns, ", "),
			strings.Join(quotes, ", "),
		)
	}
	return insertSQL, values, nil
}

func (s *SQLGenerator) toUpdate(e core.Event) (string, []interface{}, error) {
	primaryKeys := s.schema.GetPrimaryKeyNames()
	setClauses := make([]string, 0, len(e.Record.Columns)-len(primaryKeys))
	params := make([]interface{}, 0, len(e.Record.Columns)-len(primaryKeys))
	whereClauses := make([]string, 0, len(primaryKeys))
	whereValues := make([]interface{}, 0, len(primaryKeys))

	for _, col := range e.Record.Columns {
		isPK := false
		for _, pk := range primaryKeys {
			if pk == col.Name {
				isPK = true
				break
			}
		}
		v, err := s.typeMap.Decode(col.Value)
		if err != nil {
			return "", nil, errors.New(fmt.Sprintf("field %s decode fail,%s", col.Name, err.Error()))
		}
		if isPK {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", s.quote(col.Name)))
			whereValues = append(whereValues, v)
		} else {
			setClauses = append(setClauses, fmt.Sprintf("%s = ?", s.quote(col.Name)))
			params = append(params, v)
		}
	}
	updateSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		s.quote(s.schema.Name),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)
	params = append(params, whereValues...)
	return updateSQL, params, nil
}

func (s *SQLGenerator) toDelete(e core.Event) (string, []interface{}, error) {

	var whereClauses []string
	var values []interface{}

	for _, col := range s.schema.GetPrimaryKeyNames() {
		v, err := e.Record.FieldByName(col)
		if err != nil {
			return "", nil, err
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", s.quote(col)))
		vv, err := s.typeMap.Decode(v.Value)
		if err != nil {
			return "", nil, err
		}
		values = append(values, vv)
	}
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", s.quote(e.SourceTableName), strings.Join(whereClauses, " AND "))
	return sql, values, nil
}

type CreateTableFieldDescriptionBuilder func(col schemas.Column) string

func (s *SQLGenerator) CreateTable(sourceSch *schemas.Table, fieldBuilder CreateTableFieldDescriptionBuilder) (string, error) {
	rawSQL := "CREATE TABLE IF NOT EXISTS " + s.quote(s.schema.Name) + " ("
	var columns []string
	var pkColumns []string
	for _, f := range sourceSch.GetPrimaryKeyNames() {
		pkColumns = append(pkColumns, s.quote(f))
	}
	for _, field := range sourceSch.Columns {
		columns = append(columns, fieldBuilder(field))
	}
	columns = append(columns, "PRIMARY KEY ("+strings.Join(pkColumns, ", ")+")")
	rawSQL += strings.Join(columns, ",")
	rawSQL += ");"
	return rawSQL, nil
}

func (s *SQLGenerator) quote(field string) string {
	return fmt.Sprintf("%s%s%s", sqlQuotes[s.connector.Type], field, sqlQuotes[s.connector.Type])
}
