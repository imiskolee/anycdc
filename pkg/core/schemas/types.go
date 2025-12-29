package schemas

type Type uint
type SecondlyType uint

const (
	TypeUnknown Type = iota
	TypeNull
	TypeInt
	TypeUint
	TypeDecimal
	TypeString
	TypeBool
	TypeDate
	TypeTime
	TypeTimestamp
	TypeJSON
	TypeUUID
	TypeBlob
)

const (
	SecondlyTypeUnknown SecondlyType = iota

	SecondlyTypeSmallInt
	SecondlyTypeMediumInt
	SecondlyTypeBigInt

	SecondlyTypeFloat
	SecondlyTypeReal
	SecondlyTypeDecimal

	SecondlyTypeVarChar
	SecondlyTypeChar
	SecondlyTypeSmallText
	SecondlyTypeMediumText
	SecondlyTypeLongText
	SecondlyTypeText

	SecondlyTypeSmallBlob
	SecondlyTypeMediumBlob
	SecondlyTypeLongBlob
	SecondlyTypeBlob

	SecondlyTypeTimestampWithTZ
)
