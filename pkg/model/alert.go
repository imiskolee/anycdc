package model

type Alert struct {
	Base
	Name       string `gorm:"column:name;type:varchar(255)" json:"name"`
	TaskID     string `gorm:"column:task_id;type:varchar(32)" json:"task_id"`
	Type       string `gorm:"column:type;type:varchar(32)" json:"type"`
	WebhookURL string `gorm:"column:webhook_url;type:varchar(255)" json:"webhook_url"`
}

func (s *Alert) TableName() string {
	return "alerts"
}
