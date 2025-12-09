package postgres

import (
	"github.com/imiskolee/anycdc/pkg/core/types"
	"github.com/jackc/pgx/v5/pgtype"
)

var typeMapping = map[types.Type][]uint32{
	types.TypeInt:       {pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID},
	types.TypeString:    {pgtype.UUIDOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.QCharOID, pgtype.TextOID},
	types.TypeBool:      {pgtype.BoolOID},
	types.TypeJSON:      {pgtype.JSONOID},
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
