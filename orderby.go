package query

import (
	"fmt"
)

type Order struct {
	field string
	desc  bool
}

func OrderByDir(field string, desc bool) Order {
	return Order{
		field: field,
		desc:  desc,
	}
}

func OrderBy(field string) Order {
	return OrderByDir(field, false)
}

func OrderByDesc(field string) Order {
	return OrderByDir(field, true)
}

func (o Order) IsEmpty() bool {
	return o.field == ""
}

func (o Order) generate() string {
	if o.desc {
		return fmt.Sprintf("%s DESC", o.field)
	}

	return o.field
}
