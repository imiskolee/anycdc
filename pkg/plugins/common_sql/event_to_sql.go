package common_sql

import (
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/model"
	"strings"
)

var sqlQuotes = map[string]string{
	"mysql":     "`",
	"postgres":  `"`,
	"starrocks": "`",
}

func quoted(typ string, field string) string {
	return fmt.Sprintf("%s%s%s", sqlQuotes[typ], field, sqlQuotes[typ])
}

func EventToSQL(typ string, e *core.Event) (string, []interface{}) {
	switch e.Type {
	case core.EventTypeInsert:
		return insertToSQL(typ, e)
	case core.EventTypeUpdate:
		return updateToSQL(typ, e)
	case core.EventTypeDelete:
		return deleteToSQL(typ, e)
	}
	return "", nil
}

func insertToSQL(typ string, e *core.Event) (string, []interface{}) {

	columns := make([]string, 0, len(e.Payload))
	values := make([]interface{}, 0, len(e.Payload))
	updateValues := make([]interface{}, 0, len(e.Payload)-len(e.PrimaryKeys))
	updateClauses := make([]string, 0, len(e.Payload)-len(e.PrimaryKeys))

	for col, val := range e.Payload {
		columns = append(columns, quoted(typ, col))
		values = append(values, val)
		isPK := false
		for _, pk := range e.PrimaryKeys {
			if pk.Name == col {
				isPK = true
				break
			}
		}
		if !isPK {
			updateClauses = append(updateClauses, fmt.Sprintf("%s= ?", quoted(typ, col)))
			updateValues = append(updateValues, val)
		}
	}
	quotes := make([]string, 0, len(columns))
	for i := 0; i < len(columns); i++ {
		quotes = append(quotes, "?")
	}

	insertSQL := ""

	if typ == model.ConnectorTypeMySQL {
		insertSQL = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s ",
			quoted(typ, e.Table),
			strings.Join(columns, ", "),
			strings.Join(quotes, ", "),
			strings.Join(updateClauses, ", "),
		)
		values = append(values, updateValues...)
	}

	if typ == model.ConnectorTypePostgres {
		pks := make([]string, 0, len(e.PrimaryKeys))
		for _, pk := range e.PrimaryKeys {
			pks = append(pks, quoted(typ, pk.Name))
		}
		insertSQL = fmt.Sprintf(
			`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s`,
			quoted(typ, e.Table),
			strings.Join(columns, ", "),
			strings.Join(quotes, ", "),
			strings.Join(pks, ", "),
			strings.Join(updateClauses, ", "),
		)
		values = append(values, updateValues...)
	}

	if typ == model.ConnectorTypeStarRocks {
		insertSQL = fmt.Sprintf(
			"INSERT OVERWRITE %s (%s) VALUES (%s)",
			quoted(typ, e.Table),
			strings.Join(columns, ", "),
			strings.Join(quotes, ", "),
		)
	}

	return insertSQL, ConvertToSQL(values)
}

func updateToSQL(typ string, e *core.Event) (string, []interface{}) {
	setClauses := make([]string, 0, len(e.Payload)-len(e.PrimaryKeys))
	params := make([]interface{}, 0, len(e.Payload)-len(e.PrimaryKeys))
	whereClauses := make([]string, 0, len(e.PrimaryKeys))
	whereValues := make([]interface{}, 0, len(e.PrimaryKeys))

	for col, val := range e.Payload {
		isPK := false
		for _, pk := range e.PrimaryKeys {
			if pk.Name == col {
				isPK = true
				break
			}
		}
		if isPK {
			whereClauses = append(whereClauses, quoted(typ, col))
			whereValues = append(whereValues, val)
		} else {
			setClauses = append(setClauses, fmt.Sprintf("%s = ?", quoted(typ, col)))
			params = append(params, val)
		}
	}

	updateSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		quoted(typ, e.Table),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)
	return updateSQL, ConvertToSQL(append(params, whereValues...))
}

func deleteToSQL(typ string, e *core.Event) (string, []interface{}) {

	return "", nil
}
