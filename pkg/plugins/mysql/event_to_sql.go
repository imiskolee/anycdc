package mysql

import (
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/plugins/common_sql"
	"strings"
)

const (
	sqlQuote = "`"
)

func eventToSQL(e *core.Event) (string, []interface{}) {
	switch e.Type {
	case core.EventTypeInsert:
		return insertToSQL(e)
	case core.EventTypeUpdate:
		return updateToSQL(e)
	case core.EventTypeDelete:
		return deleteToSQL(e)
	}
	return "", nil
}

func insertToSQL(e *core.Event) (string, []interface{}) {

	columns := make([]string, 0, len(e.Payload))
	values := make([]interface{}, 0, len(e.Payload))
	updateValues := make([]interface{}, 0, len(e.Payload)-len(e.PrimaryKeys))
	updateClauses := make([]string, 0, len(e.Payload)-len(e.PrimaryKeys))

	for col, val := range e.Payload {
		columns = append(columns, fmt.Sprintf("%s%s%s", sqlQuote, col, sqlQuote))
		values = append(values, val)
		isPK := false
		for _, pk := range e.PrimaryKeys {
			if pk.Name == col {
				isPK = true
				break
			}
		}
		if !isPK {
			updateClauses = append(updateClauses, fmt.Sprintf("%s%s%s = ?", sqlQuote, col, sqlQuote))
			updateValues = append(updateValues, val)
		}
	}
	quotes := make([]string, 0, len(columns))
	for i := 0; i < len(columns); i++ {
		quotes = append(quotes, "?")
	}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s%s%s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		sqlQuote,
		e.Table,
		sqlQuote,
		strings.Join(columns, ", "),
		strings.Join(quotes, ", "),
		strings.Join(updateClauses, ", "),
	)
	params := append(values, updateValues...)
	return insertSQL, common_sql.ConvertToSQL(params)
}

func updateToSQL(e *core.Event) (string, []interface{}) {
	setClauses := make([]string, 0, len(e.Payload)-len(e.PrimaryKeys))
	params := make([]interface{}, 0, len(e.Payload)-len(e.PrimaryKeys))
	whereClauses := make([]string, 0, len(e.PrimaryKeys))
	whereValues := make([]interface{}, 0, len(e.PrimaryKeys))

	for col, val := range e.Payload {
		isPK := false
		for _, pk := range e.PrimaryKeys {
			if pk.Name == col {
				break
			}
		}
		if isPK {
			whereClauses = append(whereClauses, fmt.Sprintf("%s%s%s = ?", sqlQuote, col, sqlQuote))
			whereValues = append(whereValues, val)
		} else {
			setClauses = append(setClauses, fmt.Sprintf("%s%s%s = ?", sqlQuote, col, sqlQuote))
			params = append(params, val)
		}
	}

	updateSQL := fmt.Sprintf("UPDATE %s%s%s SET %s WHERE %s",
		sqlQuote,
		e.Table,
		sqlQuote,
		strings.Join(whereClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)
	return updateSQL, common_sql.ConvertToSQL(append(params, whereValues...))
}

func deleteToSQL(e *core.Event) (string, []interface{}) {

	return "", nil
}
