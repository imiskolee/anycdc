package tests

import (
	"crypto/rand"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"math"
	"math/big"
	rand2 "math/rand"
	"net"
	"time"
)

type BasicType struct {
	// 基础字段（GORM必备）
	ID               string         `gorm:"column:id;type:uuid;primaryKey;" json:"id"`
	FieldSmallInt    int16          `gorm:"type:int;" json:"field_small_int"`
	FieldInteger     int32          `gorm:"type:integer;" json:"field_integer"`
	FieldBigInt      int64          `gorm:"type:bigint;" json:"field_big_int"`
	FieldSerial      uint32         `gorm:"type:int;" json:"field_serial"`
	FieldBigSerial   uint64         `gorm:"type:int;" json:"field_big_serial"`
	FieldNumeric     float64        `gorm:"type:numeric(10,2);" json:"field_numeric"`
	FieldReal        float32        `gorm:"type:real;" json:"field_real"`
	FieldDoublePrec  float64        `gorm:"type:double precision;" json:"field_double_prec"`
	FieldDecimal     float64        `gorm:"type:decimal(15,5);" json:"field_decimal"`
	FieldVarchar     string         `gorm:"type:varchar(100);" json:"field_varchar"`
	FieldChar        string         `gorm:"type:char(10);" json:"field_char"`
	FieldText        string         `gorm:"type:text;" json:"field_text"`
	FieldBytea       []byte         `gorm:"type:bytea;" json:"field_bytea"`
	FieldBoolean     bool           `gorm:"type:boolean;" json:"field_boolean"`
	FieldDate        time.Time      `gorm:"type:date;" json:"field_date"`
	FieldTime        time.Time      `gorm:"type:time;" json:"field_time"`
	FieldTimestamp   time.Time      `gorm:"type:timestamp;" json:"field_timestamp"`
	FieldTimestamptz time.Time      `gorm:"type:timestamptz;" json:"field_timestamptz"`
	FieldInterval    string         `gorm:"type:interval;" json:"field_interval"`
	FieldJson        JSONB          `gorm:"type:json;" json:"field_json"`
	FieldJsonb       JSONB          `gorm:"type:jsonb;" json:"field_jsonb"`
	FieldUUID        string         `gorm:"type:uuid;" json:"field_uuid"`
	CreatedAt        time.Time      `gorm:"" json:"created_at"`
	UpdatedAt        time.Time      `gorm:"" json:"updated_at"`
	DeletedAt        gorm.DeletedAt `gorm:"index;" json:"deleted_at,omitempty"`
}

func (*BasicType) TableName() string {
	return "basic_types"
}

type JSONB map[string]interface{}

// Value 将JSONB转换为数据库值
func (j JSONB) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}

// Scan 将数据库值转换为JSONB
func (j *JSONB) Scan(value interface{}) error {
	if value == nil {
		*j = make(JSONB)
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return json.Unmarshal(bytes, j)
	}

	return json.Unmarshal(bytes, j)
}

func GenerateRandomBasicType() *BasicType {
	now := time.Now()
	return &BasicType{
		ID: uuid.New().String(),
		// 数值类型
		FieldSmallInt:   generateRandomSmallInt(),
		FieldInteger:    generateRandomInteger(),
		FieldBigInt:     generateRandomBigInt(),
		FieldSerial:     uint32(generateRandomInteger()),
		FieldBigSerial:  uint64(generateRandomBigInt()),
		FieldNumeric:    generateRandomNumeric(10, 2),
		FieldReal:       generateRandomReal(),
		FieldDoublePrec: generateRandomDoublePrec(),
		FieldDecimal:    generateRandomNumeric(15, 5),

		// 字符类型
		FieldVarchar: generateRandomVarchar(100),
		FieldChar:    generateRandomChar(10),
		FieldText:    generateRandomText(1000),
		FieldBytea:   generateRandomBytea(256),

		// 布尔类型
		FieldBoolean: true,

		// 日期时间类型
		FieldDate:        generateRandomDate(now),
		FieldTime:        generateRandomTime(now),
		FieldTimestamp:   generateRandomTimestamp(now),
		FieldTimestamptz: generateRandomTimestamp(now),
		FieldInterval:    generateRandomInterval(),

		// JSON/JSONB类型
		FieldJson:  generateRandomJSONB(),
		FieldJsonb: generateRandomJSONB(),

		// 其他特殊类型
		FieldUUID: generateRandomUUID(),

		// GORM时间字段（使用当前时间）
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// -------------------------- 基础类型随机生成函数 --------------------------

// generateRandomSmallInt 生成随机smallint (-32768 ~ 32767)
func generateRandomSmallInt() int16 {
	return int16(rand2.Int31n(65535) - 32768)
}

// generateRandomInteger 生成随机integer (-2147483648 ~ 2147483647)
func generateRandomInteger() int32 {
	return rand2.Int31()
}

// generateRandomBigInt 生成随机bigint (-9223372036854775808 ~ 9223372036854775807)
func generateRandomBigInt() int64 {
	return rand2.Int63()
}

// generateRandomNumeric 生成指定精度的随机数值
func generateRandomNumeric(precision, scale int) float64 {
	max := math.Pow10(precision - scale)
	val := rand2.Float64() * max
	roundFactor := math.Pow10(int(float64(scale)))
	return math.Round(val*roundFactor) / roundFactor
}

// generateRandomReal 生成随机real (float32)
func generateRandomReal() float32 {
	return rand2.Float32() * 1000
}

// generateRandomDoublePrec 生成随机double precision (float64)
func generateRandomDoublePrec() float64 {
	return rand2.Float64() * 1000000
}

// -------------------------- 字符类型随机生成函数 --------------------------

// generateRandomVarchar 生成指定长度的随机字符串
func generateRandomVarchar(maxLen int32) string {
	length := rand2.Int31n(maxLen) + 1
	chars := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
	result := make([]rune, length)
	for i := range result {
		result[i] = chars[rand2.Int31n(int32(len(chars)))]
	}
	return string(result)
}

// generateRandomChar 生成定长随机字符串
func generateRandomChar(length int) string {
	chars := []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	result := make([]rune, length)
	for i := range result {
		result[i] = chars[rand2.Int31n(int32(len(chars)))]
	}
	return string(result)
}

// generateRandomText 生成随机长文本
func generateRandomText(maxLen int) string {
	paragraphs := int(rand2.Int31n(5) + 1)
	text := ""
	for i := 0; i < paragraphs; i++ {
		sentences := int(rand2.Int31n(8) + 2)
		for j := 0; j < sentences; j++ {
			wordCount := int(rand2.Int31n(15) + 3)
			sentence := ""
			for k := 0; k < wordCount; k++ {
				wordLen := rand2.Int31n(10) + 2
				word := generateRandomVarchar(wordLen)
				sentence += word + " "
			}
			// 结尾加标点
			punct := []string{".", "!", "?"}[rand2.Int31n(3)]
			sentence = sentence[:len(sentence)-1] + punct + " "
			text += sentence
		}
		text += "\n\n"
	}
	// 限制总长度
	if len(text) > maxLen {
		text = text[:maxLen]
	}
	return text
}

// generateRandomBytea 生成随机二进制数据
func generateRandomBytea(maxLen int) []byte {
	length := rand2.Int31n(int32(maxLen)) + 1
	bytes := make([]byte, length)
	rand.Read(bytes)
	return bytes
}

// -------------------------- 日期时间类型随机生成函数 --------------------------

// generateRandomDate 生成随机日期（近10年）
func generateRandomDate(now time.Time) time.Time {
	days := rand2.Int31n(3650) // 近10年
	return now.AddDate(0, 0, int(-days))
}

// generateRandomTime 生成随机时间
func generateRandomTime(now time.Time) time.Time {
	hours := rand2.Intn(24)
	minutes := rand2.Intn(60)
	seconds := rand2.Intn(60)
	return time.Date(1, 1, 1, hours, minutes, seconds, 0, time.UTC)
}

// generateRandomTimestamp 生成随机时间戳（近1年）
func generateRandomTimestamp(now time.Time) time.Time {
	hours := rand2.Intn(8760) // 近1年
	return now.Add(-time.Duration(hours) * time.Hour)
}

// generateRandomInterval 生成随机时间间隔字符串
func generateRandomInterval() string {
	units := []struct {
		name string
		max  int
	}{
		{"year", 10},
		{"month", 12},
		{"day", 30},
		{"hour", 24},
		{"minute", 60},
		{"second", 60},
	}

	unit := units[rand2.Intn(len(units))]
	value := rand2.Intn(unit.max) + 1
	return fmt.Sprintf("%d %s", value, unit.name)
}

// -------------------------- 数组类型随机生成函数 --------------------------

// generateRandomIntArray 生成随机整数数组
func generateRandomIntArray(maxLen int) []int {
	length := rand2.Intn(maxLen) + 1
	array := make([]int, length)
	for i := range array {
		array[i] = int(rand2.Intn(1000))
	}
	return array
}

// generateRandomStringArray 生成随机字符串数组
func generateRandomStringArray(maxLen int) []string {
	length := rand2.Intn(maxLen) + 1
	array := make([]string, length)
	for i := range array {
		array[i] = generateRandomVarchar(20)
	}
	return array
}

// generateRandomFloatArray 生成随机浮点数数组
func generateRandomFloatArray(maxLen int) []float64 {
	length := rand2.Intn(maxLen) + 1
	array := make([]float64, length)
	for i := range array {
		array[i] = generateRandomNumeric(8, 2)
	}
	return array
}

// -------------------------- JSONB类型随机生成函数 --------------------------

// generateRandomJSONB 生成随机JSONB数据
func generateRandomJSONB() JSONB {
	jsonData := make(JSONB)

	// 基础字段
	jsonData["id"] = rand2.Int63()
	jsonData["name"] = generateRandomVarchar(20)
	jsonData["email"] = fmt.Sprintf("%s@%s.com", generateRandomVarchar(10), generateRandomVarchar(8))
	jsonData["is_active"] = true
	jsonData["score"] = generateRandomNumeric(5, 1)

	// 嵌套对象
	address := make(JSONB)
	address["street"] = generateRandomVarchar(30)
	address["city"] = generateRandomVarchar(15)
	address["zipcode"] = generateRandomChar(6)
	jsonData["address"] = address

	// 数组
	hobbies := []string{}
	for i := 0; i < rand2.Intn(5)+1; i++ {
		hobbies = append(hobbies, generateRandomVarchar(10))
	}
	jsonData["hobbies"] = hobbies

	// 时间
	jsonData["created_at"] = time.Now().Format(time.RFC3339)

	return jsonData
}

// -------------------------- 几何类型随机生成函数 --------------------------

// generateRandomPoint 生成随机点 (x,y)
func generateRandomPoint() string {
	x := generateRandomNumeric(6, 2)
	y := generateRandomNumeric(6, 2)
	return fmt.Sprintf("(%f, %f)", x, y)
}

// generateRandomLine 生成随机直线
func generateRandomLine() string {
	x1 := generateRandomNumeric(6, 2)
	y1 := generateRandomNumeric(6, 2)
	x2 := generateRandomNumeric(6, 2)
	y2 := generateRandomNumeric(6, 2)
	return fmt.Sprintf("{%f,%f,%f,%f}", x1, y1, x2, y2)
}

// generateRandomCircle 生成随机圆
func generateRandomCircle() string {
	x := generateRandomNumeric(6, 2)
	y := generateRandomNumeric(6, 2)
	r := generateRandomNumeric(4, 2) + 1 // 半径至少1
	return fmt.Sprintf("<(%f,%f),%f>", x, y, r)
}

// -------------------------- 范围类型随机生成函数 --------------------------

// generateRandomInt4Range 生成随机整数范围
func generateRandomInt4Range() string {
	start := int(generateRandomInteger())
	end := start + rand2.Intn(100) + 10
	// 随机选择闭区间/开区间
	startChar := []string{"[", "("}[rand2.Intn(2)]
	endChar := []string{"]", ")"}[rand2.Intn(2)]
	return fmt.Sprintf("%s%d,%d%s", startChar, start, end, endChar)
}

// generateRandomNumRange 生成随机数值范围
func generateRandomNumRange() string {
	start := generateRandomNumeric(8, 2)
	end := start + generateRandomNumeric(4, 2) + 1
	startChar := []string{"[", "("}[rand2.Intn(2)]
	endChar := []string{"]", ")"}[rand2.Intn(2)]
	return fmt.Sprintf("%s%f,%f%s", startChar, start, end, endChar)
}

// generateRandomTsRange 生成随机时间戳范围
func generateRandomTsRange(now time.Time) string {
	start := generateRandomTimestamp(now)
	end := start.Add(time.Duration(rand2.Intn(1000)) * time.Hour)
	startChar := []string{"[", "("}[rand2.Intn(2)]
	endChar := []string{"]", ")"}[rand2.Intn(2)]
	return fmt.Sprintf("%s'%s','%s'%s",
		startChar,
		start.Format(time.RFC3339),
		end.Format(time.RFC3339),
		endChar,
	)
}

// -------------------------- 其他特殊类型随机生成函数 --------------------------

// generateRandomUUID 生成随机UUID
func generateRandomUUID() string {
	return uuid.New().String()
}

// generateRandomInet 生成随机IP地址 (v4/v6)
func generateRandomInet() string {
	if rand2.Float64() > 0.5 {
		// IPv4
		ip := make(net.IP, 4)
		rand.Read(ip)
		return ip.String()
	}
	// IPv6
	ip := make(net.IP, 16)
	rand.Read(ip)
	return ip.String()
}

// generateRandomCidr 生成随机CIDR地址
func generateRandomCidr() string {
	ip := make(net.IP, 4)
	rand.Read(ip)
	mask := rand2.Intn(30) + 2 // /2 ~ /30
	return fmt.Sprintf("%s/%d", ip.String(), mask)
}

// generateRandomMacAddr 生成随机MAC地址
func generateRandomMacAddr() string {
	mac := make([]byte, 6)
	rand.Read(mac)
	// 确保是合法的MAC地址（第2位设置为0）
	mac[0] &= 0xfe
	return net.HardwareAddr(mac).String()
}

// generateRandomXML 生成随机XML字符串
func generateRandomXML() string {
	root := generateRandomVarchar(10)
	child := generateRandomVarchar(8)
	value := generateRandomVarchar(50)

	return fmt.Sprintf(`<%s>
  <%s id="%d">%s</%s>
  <created_at>%s</created_at>
  <is_valid>%t</is_valid>
</%s>`,
		root,
		child,
		rand2.Intn(1000),
		value,
		child,
		time.Now().Format(time.RFC3339),
		true,
		root,
	)
}

// -------------------------- 辅助函数 --------------------------

// SecureRandomInt 安全的随机整数生成（用于敏感场景）
func SecureRandomInt(max int64) (int64, error) {
	bigMax := big.NewInt(max)
	n, err := rand.Int(rand.Reader, bigMax)
	if err != nil {
		return 0, err
	}
	return n.Int64(), nil
}

// GenerateRandomBasicTypeBatch 批量生成随机BasicType
func GenerateRandomBasicTypeBatch(count int) []*BasicType {
	batch := make([]*BasicType, count)
	for i := 0; i < count; i++ {
		batch[i] = GenerateRandomBasicType()
	}
	return batch
}
