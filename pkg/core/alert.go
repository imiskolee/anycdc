package core

import "github.com/imiskolee/anycdc/pkg/model"

type AlertManager struct {
	WebhookURL string
}

func (s *AlertManager) Scan() error {
	return nil
}

func (s *AlertManager) triggerTask(task *model.Task) error {
	return nil
}
