package writer

import (
	"bindolabs/anycdc/pkg/event"
	"fmt"
	"strings"
)

func EventToSQL(e event.Event) (string, []interface{}) {
	switch e.Type {
	case event.TypeInsert:
		return insertEventToSQL(e)
	case event.TypeUpdate:
		return updateEventToSQL(e)
	}
	return "", nil
}

func insertEventToSQL(event event.Event) (string, []interface{}) {
	if len(event.Payload) == 0 {
		return "", nil
	}

	// 提取字段名和值
	columns := make([]string, 0, len(event.Payload))
	values := make([]interface{}, 0, len(event.Payload))
	updateClauses := make([]string, 0, len(event.Payload)-1)
	updateValues := make([]interface{}, 0, len(event.Payload)-1)
	for col, val := range event.Payload {
		columns = append(columns, col)
		values = append(values, val)
		if col != event.PrimaryKey {
			updateClauses = append(updateClauses, fmt.Sprintf("`%s` = ?", col))
			updateValues = append(updateValues, val)
		}
	}

	// 拼接 SQL
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		event.Table,
		strings.Join(columns, ", "),
		strings.Repeat("?, ", len(values))[:len(values)*2-2], // 生成 "?, ?, ..."
		strings.Join(updateClauses, ", "),
	)
	params := append(values, updateValues...)

	return insertSQL, params
}

func updateEventToSQL(event event.Event) (string, []interface{}) {

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
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", col)) // 字段加反引号避免关键字冲突
		params = append(params, val)
	}

	whereClauses := fmt.Sprintf("%s = ?", event.PrimaryKey)
	params = append(params, event.PrimaryKeyValue)

	updateSQL := fmt.Sprintf(
		"UPDATE `%s` SET %s WHERE %s",
		event.Table,
		strings.Join(setClauses, ", "),
		whereClauses,
	)

	return updateSQL, params
}
