package event

type Type int

const (
	TypeInsert Type = iota + 1
	TypeUpdate
	TypeDelete
)
