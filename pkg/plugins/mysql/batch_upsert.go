package mysql

import (
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/types"
	"github.com/imiskolee/anycdc/pkg/model"
	"strings"
)

func batchUpsert(connector *model.Connector, sch *core.SimpleTableSchema, typeMap *types.Map, records []core.EventRecord) (string, []interface{}, error) {
	primaryKeys := sch.GetPrimaryKeys()
	record := records[0]
	columns := make([]string, 0, len(record.Columns))
	values := make([]interface{}, 0, len(columns)*len(records))
	placeHolders := make([]string, 0, len(records))
	updateClause := make([]string, 0, len(record.Columns)-len(primaryKeys))

	for _, col := range record.Columns {
		columns = append(columns, fmt.Sprintf("`%s`", col.Name))
		isPk := false
		for _, primaryKey := range primaryKeys {
			if col.Name == primaryKey {
				isPk = true
				break
			}
		}
		if !isPk {
			updateClause = append(updateClause, fmt.Sprintf("`%s` = VALUES(`%s`)", col.Name, col.Name))
		}
	}
	for _, record := range records {
		var rowPlaceHolders []string
		for _, column := range record.Columns {
			val, err := typeMap.Decode(column.Value)
			if err != nil {
				return "", nil, err
			}
			rowPlaceHolders = append(rowPlaceHolders, "?")
			values = append(values, val)
		}
		placeHolders = append(placeHolders, fmt.Sprintf("(%s)", strings.Join(rowPlaceHolders, ",")))
	}
	var rawSQL string
	if connector.Type == model.ConnectorTypeMySQL {
		rawSQL = fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			sch.Name,
			strings.Join(columns, ","),
			strings.Join(placeHolders, ","),
			strings.Join(updateClause, ","),
		)
	} else if connector.Type == model.ConnectorTypeStarRocks {
		rawSQL = fmt.Sprintf("INSERT OVERWRITE `%s` (%s) VALUES (%s)",
			sch.Name,
			strings.Join(columns, ","),
			strings.Join(placeHolders, ","),
		)
	}
	return rawSQL, values, nil
}
