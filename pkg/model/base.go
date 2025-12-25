package model

import (
	"gorm.io/gorm"
	"time"
)

type Base struct {
	ID        string          `gorm:"column:id;type:uuid;primary_key" json:"id"`
	CreatedAt time.Time       `gorm:"column:created_at;type:timestamp" json:"created_at"`
	UpdatedAt time.Time       `gorm:"column:updated_at;type:timestamp" json:"updated_at"`
	DeletedAt *gorm.DeletedAt `gorm:"column:deleted_at;type:timestamp" json:"-"`
}
