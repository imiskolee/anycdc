package config

import "fmt"

type ConnectorType string

const (
	ConnectorTypeMySQL     ConnectorType = "mysql"
	ConnectorTypePostgres  ConnectorType = "postgres"
	ConnectorTypeStarRocks ConnectorType = "star_rocks"
)

type Connector struct {
	Type     ConnectorType     `yaml:"type"`
	Alias    string            `yaml:"alias"`
	Host     string            `yaml:"host"`
	Port     int               `yaml:"port"`
	Username string            `yaml:"username"`
	Password string            `yaml:"password"`
	Database string            `yaml:"database"`
	Extras   map[string]string `yaml:"extras"`
}

func (s Connector) String() string {
	return fmt.Sprintf("host=%s,port=%d,database=%s", s.Host, s.Port, s.Database)
}
