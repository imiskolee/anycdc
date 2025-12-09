package model

type Reader struct {
	Base
	ConnectorID string `gorm:"column:connector_id;type:varchar(255)" json:"connector_id"`
}

func (s *Reader) TableName() string {
	return "readers"
}

func GetReaderByID(id string) (*Reader, error) {
	r := &Reader{}
	if er := DB().Where("id = ?", id).Preload("Connector").First(r).Error; er != nil {
		return nil, er
	}
	return r, nil
}
