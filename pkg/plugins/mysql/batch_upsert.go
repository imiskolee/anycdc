package mysql

import (
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/core/types"
	"github.com/imiskolee/anycdc/pkg/model"
	"log"
	"strings"
	"time"
)

func batchUpsert(connector *model.Connector, sch *schemas.Table, typeMap *types.Map, records []core.EventRecord) (string, []interface{}, error) {
	primaryKeys := sch.GetPrimaryKeyNames()
	record := records[0]
	columns := make([]string, 0, len(sch.Columns))
	values := make([]interface{}, 0, len(columns)*len(records))
	placeHolders := make([]string, 0, len(records))
	updateClause := make([]string, 0, len(record.Columns)-len(primaryKeys))
	for _, col := range sch.Columns {
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
		for _, column := range sch.Columns {
			field, err := record.FieldByName(column.Name)
			var val interface{}
			if err == nil {
				val, err = typeMap.Decode(field.Value)
				if err != nil {
					log.Println("Failed to decode field ", column.Name)
					return "", nil, err
				}
			} else {
				log.Println("Failed to get field ", column.Name)
				val = nil
			}

			if connector.Type == model.ConnectorTypeStarRocks {
				if val == nil {
					if !column.Nullable {
						switch column.DataType {
						case schemas.TypeBool:
							val = false
						case schemas.TypeString:
							val = ""
						case schemas.TypeUint, schemas.TypeInt, schemas.TypeDecimal:
							val = 0
						case schemas.TypeJSON:
							val = "{}"
						case schemas.TypeTimestamp:
							val = time.Time{}
						default:
							val = ""
						}
					}
				}
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
		rawSQL = fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
			sch.Name,
			strings.Join(columns, ","),
			strings.Join(placeHolders, ","),
		)
	}
	return rawSQL, values, nil
}
