package model

type SystemSetting struct {
	Base
	AlertWebhookURL string `json:"alert_webhook_url"`
	MaxLogSize      string `json:"max_log_size"`
}
