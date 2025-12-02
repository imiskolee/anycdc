package postgres

import (
	"bindolabs/anycdc/pkg/entry"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
)

var oidTypeMap = map[entry.Type][]uint32{
	entry.TypeUUID:      {pgtype.UUIDOID},
	entry.TypeNumeric:   {pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID},
	entry.TypeString:    {pgtype.VarcharOID, pgtype.QCharOID, pgtype.BPCharOID, pgtype.TextOID},
	entry.TypeBoolean:   {pgtype.BoolOID},
	entry.TypeJSON:      {pgtype.JSONOID},
	entry.TypeDate:      {pgtype.DateOID},
	entry.TypeTime:      {pgtype.TimeOID},
	entry.TypeTimestamp: {pgtype.TimestampOID, pgtype.TimestamptzOID},
}

func getBuiltInType(oID uint32) entry.Type {
	for targetType, oids := range oidTypeMap {
		for _, o := range oids {
			if oID == o {
				return targetType
			}
		}
	}
	return entry.TypeUnknown
}

func convertToTypedData(mi *pgtype.Map, oID uint32, data []byte) (entry.TypedData, error) {
	typ := getBuiltInType(oID)
	if typ == entry.TypeUnknown {
		return entry.NewTypedData(entry.TypeString, string(data)), nil
	}
	ot, ok := mi.TypeForOID(oID)
	if !ok {
		return entry.NewTypedData(entry.TypeString, string(data)), nil
	}
	val, err := ot.Codec.DecodeValue(mi, oID, pgtype.TextFormatCode, data)
	if err != nil {
		return entry.TypedData{}, fmt.Errorf("can not decode value %s from oid %d", data, oID)
	}
	return entry.NewTypedData(typ, val), nil
}
