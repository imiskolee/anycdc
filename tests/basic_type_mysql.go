package tests

import (
	"gorm.io/gorm"
	"time"
)

type BasicTypeMySQL struct {
	// 基础字段（GORM必备）
	ID               string         `gorm:"column:id;type:char(64);primaryKey;" json:"id"`
	FieldSmallInt    int16          `gorm:"type:int;" json:"field_small_int"`
	FieldInteger     int32          `gorm:"type:int;" json:"field_integer"`
	FieldBigInt      int64          `gorm:"type:bigint;" json:"field_big_int"`
	FieldSerial      uint32         `gorm:"type:int;" json:"field_serial"`
	FieldBigSerial   uint64         `gorm:"type:int;" json:"field_big_serial"`
	FieldNumeric     float64        `gorm:"type:decimal(10,2);" json:"field_numeric"`
	FieldReal        float32        `gorm:"type:float;" json:"field_real"`
	FieldDoublePrec  float64        `gorm:"type:float;" json:"field_double_prec"`
	FieldDecimal     float64        `gorm:"type:decimal(15,5);" json:"field_decimal"`
	FieldVarchar     string         `gorm:"type:varchar(100);" json:"field_varchar"`
	FieldChar        string         `gorm:"type:char(10);" json:"field_char"`
	FieldText        string         `gorm:"type:text;" json:"field_text"`
	FieldBytea       []byte         `gorm:"type:blob;" json:"field_bytea"`
	FieldBoolean     bool           `gorm:"type:tinyint(1);" json:"field_boolean"`
	FieldDate        time.Time      `gorm:"type:date;" json:"field_date"`
	FieldTime        string         `gorm:"type:varchar(255);" json:"field_time"`
	FieldTimestamp   time.Time      `gorm:"type:datetime;" json:"field_timestamp"`
	FieldTimestamptz time.Time      `gorm:"type:datetime;" json:"field_timestamptz"`
	FieldInterval    string         `gorm:"type:string;" json:"field_interval"`
	FieldJson        JSONB          `gorm:"type:json;" json:"field_json"`
	FieldJsonb       JSONB          `gorm:"type:json;" json:"field_jsonb"`
	FieldUUID        string         `gorm:"type:char(64);" json:"field_uuid"`
	CreatedAt        time.Time      `gorm:"" json:"created_at"`
	UpdatedAt        time.Time      `gorm:"" json:"updated_at"`
	DeletedAt        gorm.DeletedAt `gorm:"index;" json:"deleted_at,omitempty"`
}

func (*BasicTypeMySQL) TableName() string {
	return "basic_types"
}
