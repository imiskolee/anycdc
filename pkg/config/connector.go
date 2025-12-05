package config

import "fmt"

type ConnectorType string

const (
	ConnectorTypeMySQL     ConnectorType = "mysql"
	ConnectorTypePostgres  ConnectorType = "postgres"
	ConnectorTypeStarRocks ConnectorType = "star_rocks"
)

type Connector struct {
	ID       string            `gorm:"column:id;PRIMARY_KEY"`
	Type     ConnectorType     `yaml:"type" gorm:"column:type;varchar(32)"`
	Alias    string            `yaml:"alias" gorm:"column:alias:varchar(32)"`
	Host     string            `yaml:"host" gorm:"column:host:varchar(256)"`
	Port     int               `yaml:"port" gorm:"column:port:int"`
	Username string            `yaml:"username" gorm:"column:username;type:varchar(255)"`
	Password string            `yaml:"password" gorm:"column:password;type:varchar(255)"`
	Database string            `yaml:"database" gorm:"column:database;type:varchar(255)"`
	Extras   map[string]string `yaml:"extras" gorm:"column:extras;type:JSON;"`
}

func (s Connector) String() string {
	return fmt.Sprintf("host=%s,port=%d,database=%s", s.Host, s.Port, s.Database)
}
