package core

import "github.com/imiskolee/anycdc/pkg/model"

type ConnectorOption struct {
	Connector *model.Connector
}

type Connector interface {
	Test() error
}
