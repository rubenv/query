package query

import (
	"fmt"
)

type Delete struct {
	Table   string
	Dialect Dialect

	where Where
}

func (d *Delete) SetDialect(dialect Dialect) *Delete {
	d.Dialect = dialect
	return d
}

func (d *Delete) Where(where Where) *Delete {
	if d.where.IsEmpty() {
		d.where = And()
		d.where.topLevel = true
	}
	d.where.children = append(d.where.children, where)
	return d
}

func (d *Delete) ToSQL() (string, []any) {
	query := ""
	vars := make([]any, 0)

	query = fmt.Sprintf("DELETE FROM %s", d.Table)
	where, whereVars := d.where.Generate(0, d.Dialect)
	if where != "" {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}
	vars = append(vars, whereVars...)

	return query, vars
}
