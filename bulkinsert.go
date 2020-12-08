package query

import (
	"errors"
	"fmt"
	"strings"
)

type BulkInsert struct {
	Table   string
	Columns []string
	Dialect Dialect
	Values  [][]interface{}
}

func (i *BulkInsert) Add(values ...interface{}) error {
	if len(values) != len(i.Columns) {
		return errors.New("Length mismatch")
	}
	i.Values = append(i.Values, values)
	return nil
}

func (i *BulkInsert) ToSQL() (string, []interface{}) {
	vars := make([]interface{}, 0)
	placeholders := make([]string, 0)

	for _, row := range i.Values {
		placeholderVars := make([]string, 0)
		for n := range i.Columns {
			placeholderVars = append(placeholderVars, i.Dialect.Placeholder(len(vars)+n))
		}
		placeholder := fmt.Sprintf("(%s)", strings.Join(placeholderVars, ", "))
		placeholders = append(placeholders, placeholder)
		vars = append(vars, row...)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", i.Table, strings.Join(i.Columns, ", "), strings.Join(placeholders, ", "))
	return query, vars
}

func (i *BulkInsert) Count() int {
	return len(i.Values)
}
