package model

type TaskLog struct {
	Base
	TaskID                      string `gorm:"column:task_id;varchar(255)" json:"task_id"`
	ReaderID                    string `gorm:"column:reader_id;varchar(255)" json:"reader_id"`
	WriterID                    string `gorm:"column:writer_id;varchar(255)" json:"writer_id"`
	Schema                      string `gorm:"column:schema;varchar(255)" json:"schema"`
	Table                       string `gorm:"column:table;varchar(255)" json:"table"`
	TotalInsertRows             uint64 `gorm:"column:total_insert_rows;type:bigint" json:"total_insert_rows"`
	TotalUpdateRows             uint64 `gorm:"column:total_update_rows;type:bigint" json:"total_update_rows"`
	TotalDeletedRows            uint64 `gorm:"column:total_deleted_rows;type:bigint" json:"total_deleted_rows"`
	TotalInsertRowsSinceStarted uint64 `gorm:"column:total_insert_rows_since_started;type:bigint" json:"total_insert_rows_since_started"`
	TotalUpdateRowsSinceStarted uint64 `gorm:"column:total_update_rows_since_started;type:bigint" json:"total_update_rows_since_started"`
	TotalDeleteRowsSinceStarted uint64 `gorm:"column:total_delete_rows_since_started;type:bigint" json:"total_delete_rows_since_started"`
}

func (*TaskLog) TableName() string {
	return "task_logs"
}
