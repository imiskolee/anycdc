package schemas

type Column struct {
	DataType         Type
	Index            uint
	SecondlyType     SecondlyType
	Name             string
	Nullable         bool
	Default          string
	IsPrimaryKey     bool
	ColumnLength     int
	NumericPrecision int
	NumericScale     int
}
