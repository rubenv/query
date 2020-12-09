package query

import "database/sql/driver"

type now struct{}

func Now() driver.Valuer {
	return &now{}
}

func (n *now) Value() (driver.Value, error) {
	return "CURRENT_TIMESTAMP", nil
}
