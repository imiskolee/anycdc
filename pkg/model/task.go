package model

import (
	"time"
)

type Metric struct {
	RawEvents        uint64
	Inserted         uint64
	Updated          uint64
	Deleted          uint64
	LastSyncAt       time.Time
	LastSyncPosition string
}

const (
	TaskStatusActive          = "Active"
	TaskStatusInactive        = "Inactive"
	TaskRunnerStatusPreparing = "Preparing"
	TaskRunnerStatusFailed    = "Failed"
	TaskRunnerStatusRunning   = "Running"
	TaskRunnerStatusStopped   = "Stopped"

	WriterPolicyRetry = "retry"
	WriterPolicyStop  = "stop"
	WriterPolicySkip  = "skip"
)

type Task struct {
	Base
	Name                          string     `gorm:"column:name;type:varchar(255)" json:"name"`
	Reader                        string     `gorm:"column:reader;type:varchar(255)" json:"reader"`
	Tables                        string     `gorm:"column:tables;type:text" json:"tables"`
	Writers                       string     `gorm:"column:writers;type:text" json:"writers"`
	Extras                        string     `gorm:"column:extras;type:text" json:"extras"`
	WriterPolicy                  string     `gorm:"column:writer_policy;type:varchar(255)" json:"writer_policy"`
	RunnerStatus                  string     `gorm:"column:runner_status;type:varchar(255)" json:"runner_status"`
	RunnerStatusFailedReason      string     `gorm:"column:runner_status_failed_reason;type:text" json:"runner_status_failed_reason"`
	BatchSize                     int        `gorm:"column:batch_size;type:int" json:"batch_size"`
	LastPosition                  string     `gorm:"column:last_position;type:varchar(255)" json:"last_position"`
	LastStarted                   *time.Time `gorm:"column:last_started;type:timestamp" json:"last_started"`
	LastSyncedAt                  *time.Time `gorm:"column:last_synced_at;type:timestamp" json:"last_synced_at"`
	LastEventAt                   *time.Time `gorm:"column:last_event_at;type:timestamp" json:"last_event_at"`
	MetricInsertCount             uint64     `gorm:"column:metric_insert_count;type:bigint;default:0" json:"metric_insert_count"`
	MetricUpdateCount             uint64     `gorm:"column:metric_update_count;type:bigint;default:0" json:"metric_update_count"`
	MetricDeleteCount             uint64     `gorm:"column:metric_delete_count;type:bigint;default:0" json:"metric_delete_count"`
	MetricInsertCountSinceStarted uint64     `gorm:"column:metric_insert_count_since_started;type:bigint;default:0" json:"metric_insert_count_since_started"`
	MetricUpdateCountSinceStarted uint64     `gorm:"column:metric_update_count_since_started;type:bigint;default:0" json:"metric_update_count_since_started"`
	MetricDeleteCountSinceStarted uint64     `gorm:"column:metric_delete_count_since_started;type:bigint;default:0" json:"metric_delete_count_since_started"`

	Status string `gorm:"column:status;type:varchar(255)" json:"status"`
}

func (s *Task) TableName() string {
	return "tasks"
}

func (s *Task) UpdateMetric(metric Metric) error {
	{
		sql := `UPDATE tasks SET 
                 metric_insert_count = metric_insert_count + ?, metric_insert_count_since_started = metric_insert_count_since_started + ?, 
                 metric_update_count = metric_update_count + ?, metric_update_count_since_started = metric_update_count_since_started + ?,
                 metric_delete_count = metric_delete_count + ?, metric_delete_count_since_started = metric_delete_count_since_started + ?,
                 last_position = ?,
                 last_synced_at=? WHERE id=?`

		if err := DB().Exec(sql,
			metric.Inserted, metric.Inserted,
			metric.Updated, metric.Updated,
			metric.Deleted, metric.Deleted,
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

func (s *Task) UpdateRunnerStatus(status string, failedReason ...string) error {
	reason := ""
	if len(failedReason) > 0 {
		reason = failedReason[0]
	}
	return s.PartialUpdates(map[string]interface{}{
		"runner_status":               status,
		"runner_status_failed_reason": reason,
	})

}

func (s *Task) PartialUpdates(val map[string]interface{}) error {
	if err := DB().Table(s.TableName()).Where("id = ?", s.ID).Updates(val).Error; err != nil {
		return err
	}
	return nil
}
