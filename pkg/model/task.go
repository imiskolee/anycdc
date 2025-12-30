package model

import (
	"strings"
	"time"
)

type TaskMetric struct {
	LastSyncAt       time.Time
	LastSyncPosition string
}

const (
	TaskStatusActive   = "Active"
	TaskStatusInactive = "Inactive"

	DumperStatusRunning   = "Running"
	DumperStatusCompleted = "Completed"
	DumperStatusFailed    = "Failed"

	CDCStatusRunning           = "Running"
	CDCStatusFailed            = "Failed"
	CDCStatusStopped           = "Stopped"
	TaskRunnerStatusPreparing  = "Preparing"
	TaskRunnerStatusFailed     = "Failed"
	TaskRunnerStatusRunning    = "Running"
	TaskRunnerStatusStopped    = "Stopped"
	TaskInitialStatusPreparing = "Preparing"
	TaskInitialStatusFailed    = "Failed"
	TaskInitialStatusRunning   = "Running"
	TaskInitialStatusCompleted = "Completed"

	EventFailPolicyRetry = "retry"
	EventFailPolicyStop  = "stop"
	EventFailPolicySkip  = "skip"

	ModeDebug  = "debug"
	ModeNormal = "normal"
)

type Task struct {
	Base
	Name            string     `gorm:"column:name;type:varchar(255)" json:"name"`
	Reader          string     `gorm:"column:reader;type:varchar(255)" json:"reader"`
	Tables          string     `gorm:"column:tables;type:text" json:"tables"`
	Writer          string     `gorm:"column:writer;type:text" json:"writer"`
	Extras          string     `gorm:"column:extras;type:text" json:"extras"`
	WriterPolicy    string     `gorm:"column:writer_policy;type:varchar(255)" json:"writer_policy"`
	DumperEnabled   bool       `gorm:"column:dumper_enabled;type:bool" json:"dumper_enabled"`
	MigrateEnabled  bool       `gorm:"column:migrate_enabled;type:bool" json:"migrate_enabled"`
	CDCEnabled      bool       `gorm:"column:cdc_enabled;type:bool" json:"cdc_enabled"`
	CDCStatus       string     `gorm:"column:cdc_status;type:varchar(255)" json:"cdc_status"`
	DebugEnabled    bool       `gorm:"column:debug_enabled;type:bool" json:"debug_enabled"`
	Message         string     `gorm:"column:message;type:text" json:"message"`
	LogMode         string     `gorm:"column:log_mode;type:varchar(255)" json:"log_mode"`
	BatchSize       int        `gorm:"column:batch_size;type:int" json:"batch_size"`
	LastCDCPosition string     `gorm:"column:last_cdc_position;type:varchar(255)" json:"last_cdc_position"`
	LastCDCAt       *time.Time `gorm:"column:last_cdc_at;type:timestamp" json:"last_cdc_at"`
	LastStarted     *time.Time `gorm:"column:last_started;type:timestamp" json:"last_started"`
	ThreadNumber    int        `gorm:"column:thread_number;type:int" json:"thread_number"`
	Status          string     `gorm:"column:status;type:varchar(255)" json:"status"`
}

func (s *Task) TableName() string {
	return "tasks"
}

func (s *Task) GetTables() []string {
	return strings.Split(s.Tables, ",")
}

func (s *Task) UpdateMetric(metric TaskMetric) error {
	{
		sql := `UPDATE tasks SET 
                 last_position = ?,
                 last_synced_at=? WHERE id=?`

		if err := DB().Exec(sql,
			metric.LastSyncPosition,
			metric.LastSyncAt,
			s.ID,
		).Error; err != nil {
			return err
		}
	}
	return nil
}

func (s *Task) UpdateStatus(status string) error {
	updates := map[string]interface{}{
		"status": status,
	}
	return s.PartialUpdates(updates)
}

func (s *Task) UpdateCDCStatus(status string) error {
	return s.PartialUpdates(map[string]interface{}{
		"cdc_status": status,
	})
}

func (s *Task) PartialUpdates(val map[string]interface{}) error {
	if err := DB().Table(s.TableName()).Where("id = ?", s.ID).Updates(val).Error; err != nil {
		return err
	}
	return nil
}
