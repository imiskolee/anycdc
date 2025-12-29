package postgres

import (
	"database/sql/driver"
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/core/types"
	"github.com/jackc/pgx/v5/pgtype"
)

var dataTypes = types.NewDefaultTypeMap()
var pgxTypeMap *pgtype.Map = pgtype.NewMap()

func convertFromPGX(oID uint32, data []byte) (types.TypedData, error) {
	typ, _ := getBuiltInType(oID)
	if len(data) == 0 {
		return types.NewNullData(), nil
	}
	if typ == schemas.TypeUnknown {
		return types.NewTypedData(schemas.TypeUnknown, string(data)), nil
	}
	ot, ok := pgxTypeMap.TypeForOID(oID)
	if !ok {
		return types.NewTypedData(schemas.TypeUnknown, string(data)), nil
	}

	val, err := ot.Codec.DecodeValue(pgxTypeMap, oID, pgtype.TextFormatCode, data)
	if err != nil {
		return types.NewNullData(), err
	}
	if valuer, ok := val.(driver.Valuer); ok {
		v, err := valuer.Value()
		if err == nil {
			val = v
		}
	}
	v, err := dataTypes.Encode(typ, val)
	if err != nil {
		return types.NewNullData(), err
	}
	return v, nil
}
