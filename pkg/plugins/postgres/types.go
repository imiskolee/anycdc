package postgres

import (
	"database/sql/driver"
	"github.com/imiskolee/anycdc/pkg/core/types"
	"github.com/jackc/pgx/v5/pgtype"
)

var dataTypes = types.NewDefaultTypeMap()
var pgxTypeMap *pgtype.Map = pgtype.NewMap()
var typeMapping = map[types.Type][]uint32{
	types.TypeInt:     {pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID},
	types.TypeDecimal: {pgtype.Float4OID, pgtype.NumericOID, pgtype.Float8OID},
	types.TypeString: {pgtype.VarcharOID, pgtype.BPCharOID, pgtype.QCharOID, pgtype.TextOID,
		pgtype.TimeOID, pgtype.IntervalOID},
	types.TypeUUID:      {pgtype.UUIDOID},
	types.TypeBool:      {pgtype.BoolOID},
	types.TypeBlob:      {pgtype.ByteaOID},
	types.TypeJSON:      {pgtype.JSONOID, pgtype.JSONBOID},
	types.TypeTimestamp: {pgtype.DateOID, pgtype.TimestampOID, pgtype.TimestamptzOID},
}

func getBuiltInType(oID uint32) types.Type {
	for targetType, oids := range typeMapping {
		for _, o := range oids {
			if oID == o {
				return targetType
			}
		}
	}
	return types.TypeUnknown
}

func convertFromPGX(oID uint32, data []byte) (types.TypedData, error) {
	typ := getBuiltInType(oID)
	if len(data) == 0 {
		return types.NewNullData(), nil
	}
	if typ == types.TypeUnknown {
		return types.NewTypedData(types.TypeUnknown, string(data)), nil
	}
	ot, ok := pgxTypeMap.TypeForOID(oID)
	if !ok {
		return types.NewTypedData(types.TypeUnknown, string(data)), nil
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
