package model

import (
	"encoding/json"
	"github.com/google/uuid"
	"time"
)

const (
	TaskModeDumper = "dumper"
	TaskModeCDC    = "cdc"

	DumperStateInitialed = "Initialed"
	DumperStateStopped   = "Stopped"
	DumperStateFailed    = "Failed"
	DumperStateRunning   = "Running"
	DumperStateCompleted = "Completed"
)

type TaskTableMetric struct {
	Mode           string
	Inserted       uint64
	Updated        uint64
	Deleted        uint64
	LastEventTime  time.Time
	LastSyncedKeys interface{}
}

type TaskTable struct {
	Base
	TaskID        string    `gorm:"column:task_id;varchar(255)" json:"task_id"`
	Schema        string    `gorm:"column:schema;varchar(255)" json:"schema"`
	Table         string    `gorm:"column:table;varchar(255)" json:"table"`
	LastDumperKey string    `gorm:"column:last_dumper_key;type:text" json:"last_dumper_key"`
	DumperState   string    `gorm:"column:dumper_state;varchar(255)" json:"dumper_state"`
	LastEventTime time.Time `gorm:"column:last_event_time" json:"last_event_time"`
	TotalDumped   uint64    `gorm:"column:total_dumped;type:bigint;default:0" json:"total_dumped"`
	TotalInserted uint64    `gorm:"column:total_inserted;type:bigint;default:0" json:"total_inserted"`
	TotalUpdated  uint64    `gorm:"column:total_updated;type:bigint;default:0" json:"total_updated"`
	TotalDeleted  uint64    `gorm:"column:total_deleted;type:bigint;default:0" json:"total_deleted"`
}

func GetOrCreateTaskTable(taskID string, table string) (*TaskTable, error) {
	var task TaskTable
	if err := DB().Where(`task_id = ? AND "table" = ?`, taskID, table).First(&task).Error; err == nil {
		return &task, nil
	}
	task.ID = uuid.New().String()
	task.TaskID = taskID
	task.Table = table
	task.TotalDumped = 0
	task.TotalInserted = 0
	task.TotalUpdated = 0
	task.TotalDeleted = 0
	task.DumperState = DumperStateInitialed
	if err := DB().Create(&task).Error; err != nil {
		return nil, err
	}
	return &task, nil
}

func (m *TaskTable) UpdateDumperState(state string) {
	DB().Model(m).Where("id = ?", m.ID).Update("dumper_state", state)
}

func (*TaskTable) TableName() string {
	return "task_tables"
}

func (m *TaskTable) Flush(metric TaskTableMetric) error {
	switch metric.Mode {
	case TaskModeDumper:
		lastState := ""
		if metric.LastSyncedKeys != nil {
			j, _ := json.Marshal(metric.LastSyncedKeys)
			lastState = string(j)
		}
		s := "UPDATE task_tables SET total_dumped = total_dumped + ?, last_dumper_key=? WHERE id = ?"
		if err := DB().Exec(s, metric.Inserted, lastState, m.ID).Error; err != nil {
			return err
		}
		break
	case TaskModeCDC:
		s := `UPDATE task_tables SET 
                     total_inserted = total_inserted + ?, 
                     total_updated = total_updated + ?,
                     total_deleted = total_deleted + ?
                     WHERE id = ?`
		if err := DB().Exec(s, metric.Inserted, metric.Updated, metric.Deleted, m.ID).Error; err != nil {
			return err
		}
		break
	}
	return nil
}
