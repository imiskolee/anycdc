package common_rds

import (
	"bindolabs/anycdc/pkg/config"
	"bindolabs/anycdc/pkg/event"
	"fmt"
	"strings"
)

var wrappers = map[config.ConnectorType]string{
	config.ConnectorTypeMySQL:    "`",
	config.ConnectorTypePostgres: "\"",
}

var duplicateClause = map[config.ConnectorType]func(e event.Event) string{
	config.ConnectorTypeMySQL: func(e event.Event) string {
		return "ON DUPLICATE KEY UPDATE"
	},
	config.ConnectorTypePostgres: func(e event.Event) string {
		return fmt.Sprintf("ON CONFLICT (\"%s\") DO UPDATE SET", e.PrimaryKey)
	},
}

func EventToSQL(destination config.ConnectorType, e event.Event) (string, []interface{}) {
	switch e.Type {
	case event.TypeInsert:
		return insertEventToSQL(destination, e)
	case event.TypeUpdate:
		return updateEventToSQL(destination, e)
	}
	return "", nil
}

func insertEventToSQL(destination config.ConnectorType, e event.Event) (string, []interface{}) {
	if len(e.Payload) == 0 {
		return "", nil
	}
	quote := wrappers[destination]
	columns := make([]string, 0, len(e.Payload))
	values := make([]interface{}, 0, len(e.Payload))
	updateValues := make([]interface{}, 0, len(e.Payload)-1)
	updateClauses := make([]string, 0, len(e.Payload)-1)

	for col, val := range e.Payload {
		columns = append(columns, fmt.Sprintf("%s%s%s", quote, col, quote))
		values = append(values, val)
		if col != e.PrimaryKey {
			updateClauses = append(updateClauses, fmt.Sprintf("%s%s%s = ?", quote, col, quote))
			updateValues = append(updateValues, val)
		}
	}

	quotes := make([]string, 0, len(columns))
	for i := 0; i < len(columns); i++ {
		quotes = append(quotes, "?")
	}
	duplicateClause := duplicateClause[destination]

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s%s%s (%s) VALUES (%s) %s %s",
		quote,
		e.Table,
		e,
		strings.Join(columns, ", "),
		strings.Join(quotes, ", "),
		duplicateClause,
		strings.Join(updateClauses, ", "),
	)
	params := append(values, updateValues...)
	return insertSQL, params
}

func updateEventToSQL(destination config.ConnectorType, e event.Event) (string, []interface{}) {
	setClauses := make([]string, 0, len(e.Payload)-1)
	params := make([]interface{}, 0, len(e.Payload))
	quote := wrappers[destination]
	for col, val := range e.Payload {
		if col == e.PrimaryKey {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s%s%s = ?", quote, col, quote)) // 字段加反引号避免关键字冲突
		params = append(params, val)
	}

	whereClauses := fmt.Sprintf("%s%s%s = ?", quote, e.PrimaryKey, quote)
	params = append(params, e.PrimaryKeyValue)

	updateSQL := fmt.Sprintf(
		"UPDATE %s%s%s SET %s WHERE %s",
		quote, e.Table, quote,
		strings.Join(setClauses, ", "),
		whereClauses,
	)

	return updateSQL, params
}
