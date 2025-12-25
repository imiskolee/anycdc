package model

const (
	ConnectorTypeMySQL     string = "mysql"
	ConnectorTypePostgres  string = "postgres"
	ConnectorTypeStarRocks string = "starrocks"

	ConnectorTargetTypeReader = "reader"
	ConnectorTargetTypeWriter = "writer"
)

type Connector struct {
	Base
	Name       string `gorm:"column:name;type:varchar(255)" json:"name" validate:"required,min=2,max=128"`
	Type       string `gorm:"column:type;type:varchar(255)" json:"type" validate:"required,min=2,max=128"`
	TargetType string `gorm:"column:target_type;varchar(255)" json:"target_type" validate:"required,min=2,max=128"`
	Host       string `gorm:"column:host;type:varchar(255)" json:"host" validate:"required,min=2,max=128"`
	Port       int    `gorm:"column:port;type:int" json:"port" validate:"required,min=1,max=65535"`
	Username   string `gorm:"column:username;type:varchar(255)" json:"username" validate:"min=0,max=128"`
	Password   string `gorm:"column:password;type:varchar(255)" json:"password" validate:"min=0,max=128"`
	Database   string `gorm:"column:database;type:varchar(255)" json:"database" validate:"min=0,max=128"`
}

func (*Connector) TableName() string {
	return "connectors"
}
