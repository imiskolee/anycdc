package writer

import (
	"fmt"
	"github.com/imiskolee/anycdc/pkg/event"
	"strings"
)

func EventToSQL(e event.Event, fieldWrapper string) (string, []interface{}) {
	switch e.Type {
	case event.TypeInsert:
		return insertEventToSQL(e, fieldWrapper)
	case event.TypeUpdate:
		return updateEventToSQL(e, fieldWrapper)
	}
	return "", nil
}

func insertEventToSQL(event event.Event, fieldWrapper string) (string, []interface{}) {
	if len(event.Payload) == 0 {
		return "", nil
	}
	// 提取字段名和值
	columns := make([]string, 0, len(event.Payload))
	values := make([]interface{}, 0, len(event.Payload))
	updateClauses := make([]string, 0, len(event.Payload)-1)
	updateValues := make([]interface{}, 0, len(event.Payload)-1)
	for col, val := range event.Payload {
		columns = append(columns, fmt.Sprintf("%s%s%s", fieldWrapper, col, fieldWrapper))
		values = append(values, val)
		if col != event.PrimaryKey {
			updateClauses = append(updateClauses, fmt.Sprintf("%s%s%s = ?", fieldWrapper, col, fieldWrapper))
			updateValues = append(updateValues, val)
		}
	}

	quotes := make([]string, 0, len(columns))
	for i := 0; i < len(columns); i++ {
		quotes = append(quotes, "?")
	}
	// ON CONFLICT (field_uuid) DO UPDATE SET
	// ON DUPLICATE KEY UPDATE

	duplicateStagement := "ON DUPLICATE KEY UPDATE"

	if fieldWrapper == "\"" {
		duplicateStagement = fmt.Sprintf("ON CONFLICT (\"%s\") DO UPDATE SET", event.PrimaryKey)
	}

	// 拼接 SQL
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s%s%s (%s) VALUES (%s) %s %s",
		fieldWrapper,
		event.Table,
		fieldWrapper,
		strings.Join(columns, ", "),
		strings.Join(quotes, ", "),
		duplicateStagement,
		strings.Join(updateClauses, ", "),
	)
	params := append(values, updateValues...)
	return insertSQL, params
}

func updateEventToSQL(event event.Event, fieldWrapper string) (string, []interface{}) {

	if len(event.Payload) == 0 {
		return "", nil
	}

	// 拼接 SET 子句
	setClauses := make([]string, 0, len(event.Payload)-1)
	params := make([]interface{}, 0, len(event.Payload))
	for col, val := range event.Payload {
		if col == event.PrimaryKey {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s%s%s = ?", fieldWrapper, col, fieldWrapper)) // 字段加反引号避免关键字冲突
		params = append(params, val)
	}

	whereClauses := fmt.Sprintf("%s%s%s = ?", fieldWrapper, event.PrimaryKey, fieldWrapper)
	params = append(params, event.PrimaryKeyValue)

	updateSQL := fmt.Sprintf(
		"UPDATE %s%s%s SET %s WHERE %s",
		fieldWrapper, event.Table, fieldWrapper,
		strings.Join(setClauses, ", "),
		whereClauses,
	)

	return updateSQL, params
}
