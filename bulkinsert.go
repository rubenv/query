package query

import (
	"errors"
	"fmt"
	"strings"
)

type BulkInsert struct {
	mode           insertUpdateMode
	Table          string
	Columns        []string
	Dialect        Dialect
	Values         [][]interface{}
	conflictColumn []string
}

func (i *BulkInsert) Add(values ...interface{}) error {
	if len(values) != len(i.Columns) {
		return errors.New("Length mismatch")
	}
	i.Values = append(i.Values, values)
	return nil
}

func (i *BulkInsert) ToSQL() (string, []interface{}) {
	switch i.mode {
	case insertMode:
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
	case upsertMode:
		vars := make([]interface{}, 0)
		for _, row := range i.Values {
			vars = append(vars, row...)
		}
		fvs := make([]fieldValue, 0)
		for _, column := range i.Columns {
			fvs = append(fvs, fieldValue{
				key: column,
			})
		}
		query := i.Dialect.MakeUpsert(i.Table, i.conflictColumn, fvs, len(i.Values))
		return query, vars
	default:
		panic(fmt.Sprintf("Unknown mode: %#v", i.mode))
	}
}

func (i *BulkInsert) Count() int {
	return len(i.Values)
}
