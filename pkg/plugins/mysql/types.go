package mysql

const (
	ColumnTypeTinyInt   = "tinyint"   // 1字节整数
	ColumnTypeSmallInt  = "smallint"  // 2字节整数
	ColumnTypeMediumInt = "mediumint" // 3字节整数
	ColumnTypeInt       = "int"       // 4字节整数（mysql 中也可用 integer 同义）
	ColumnTypeInteger   = "integer"   // int 的同义词
	ColumnTypeBigInt    = "bigint"    // 8字节整数
	ColumnTypeFloat     = "float"     // 单精度浮点数
	ColumnTypeDouble    = "double"    // 双精度浮点数（mysql 中也可用 real 同义）
	ColumnTypeReal      = "real"      // double 的同义词
	ColumnTypeDecimal   = "decimal"   // 定点数（高精度小数，mysql 中也可用 numeric 同义）
	ColumnTypeNumeric   = "numeric"   // decimal 的同义词
	ColumnTypeBit       = "bit"       // 位类型
	ColumnTypeYear      = "year"      // 年份类型
)

// 二、日期时间类型
const (
	ColumnTypeDate      = "date"      // 日期类型（YYYY-MM-DD）
	ColumnTypeTime      = "time"      // 时间类型（HH:MM:SS）
	ColumnTypeDateTime  = "datetime"  // 日期时间类型（YYYY-MM-DD HH:MM:SS）
	ColumnTypeTimestamp = "timestamp" // 时间戳类型（依赖时区，自动更新）
)

// 三、字符串/二进制类型
const (
	ColumnTypeChar       = "char"       // 固定长度字符串
	ColumnTypeVarchar    = "varchar"    // 可变长度字符串
	ColumnTypeTinyText   = "tinytext"   // 小型文本（最大255字节）
	ColumnTypeText       = "text"       // 标准文本（最大65535字节）
	ColumnTypeMediumText = "mediumtext" // 中型文本（最大16MB）
	ColumnTypeLongText   = "longtext"   // 大型文本（最大4GB）
	ColumnTypeTinyBlob   = "tinyblob"   // 小型二进制对象（最大255字节）
	ColumnTypeBlob       = "blob"       // 标准二进制对象（最大65535字节）
	ColumnTypeMediumBlob = "mediumblob" // 中型二进制对象（最大16MB）
	ColumnTypeLongBlob   = "longblob"   // 大型二进制对象（最大4GB）
	ColumnTypeEnum       = "enum"       // 枚举类型（需额外解析选项列表）
	ColumnTypeSet        = "set"        // 集合类型（需额外解析选项列表）
)

// 四、特殊类型（JSON、空间类型等）
const (
	ColumnTypeJSON               = "json"               // JSON 类型（mysql 5.7+ 支持）
	ColumnTypeGeometry           = "geometry"           // 空间几何类型（如 point、lineString 等的基础类型）
	ColumnTypePoint              = "point"              // 空间点类型
	ColumnTypeLineString         = "linestring"         // 空间线类型
	ColumnTypePolygon            = "polygon"            // 空间多边形类型
	ColumnTypeMultiPoint         = "multipoint"         // 空间多点类型
	ColumnTypeMultiLineString    = "multilinestring"    // 空间多线类型
	ColumnTypeMultiPolygon       = "multipolygon"       // 空间多多边形类型
	ColumnTypeGeometryCollection = "geometrycollection" // 空间几何集合类型
)
