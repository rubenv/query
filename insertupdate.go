package query

import (
	"fmt"
	"reflect"
	"strings"
)

type insertUpdateMode int

const (
	insertMode insertUpdateMode = iota
	updateMode
	upsertMode
)

type fieldValue struct {
	key   string
	value any
	from  *Select
}

type InsertUpdate struct {
	mode           insertUpdateMode
	Table          string
	where          Where
	fields         []fieldValue
	fromSelect     *Select
	dialect        Dialect
	conflictColumn []string
	returning      string
}

func (i *InsertUpdate) Add(key string, value any) *InsertUpdate {
	i.fields = append(i.fields, fieldValue{key: key, value: value})
	return i
}

func (i *InsertUpdate) AddSelect(key string, s *Select) *InsertUpdate {
	i.fields = append(i.fields, fieldValue{key: key, from: s})
	return i
}

func (i *InsertUpdate) addStructFields(options *InsertUpdateOptions, t reflect.Type, v reflect.Value) {
	for j := 0; j < t.NumField(); j++ {
		field := t.Field(j)
		if field.Anonymous {
			i.addStructFields(options, field.Type, v.Field(j))
		}
		tag := field.Tag.Get("db")
		parts := strings.Split(tag, ",")
		if parts[0] == "" || parts[0] == "-" {
			continue
		}
		if len(parts) > 1 {
			if parts[1] == "autoincrement" {
				val := v.Field(j)
				if (i.mode != insertMode || val.IsZero()) && !options.CopyAutoIncrement {
					continue
				}
			}
			if parts[1] == "readonly" && !options.CopyReadOnly {
				continue
			}
		}
		i.Add(parts[0], v.Field(j).Interface())
	}
}

func (i *InsertUpdate) With(obj any, opts ...WithOpt) *InsertUpdate {
	options := &InsertUpdateOptions{}
	for _, o := range opts {
		o(options)
	}

	v := reflect.ValueOf(obj)
	if v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()
	if t.Kind() == reflect.Struct {
		i.addStructFields(options, t, v)
	}
	return i
}

func (i *InsertUpdate) Select(s *Select) *InsertUpdate {
	i.fromSelect = s
	return i
}

func (i *InsertUpdate) Returning(field string) *InsertUpdate {
	i.returning = field
	return i
}

func (i *InsertUpdate) ToSQL() (string, []any) {
	query := ""
	vars := make([]any, 0)
	whereOffset := 0
	for _, v := range i.fields {
		if v.from == nil {
			vars = append(vars, v.value)
			whereOffset += 1
		}
	}

	switch i.mode {
	case insertMode:
		if i.fromSelect != nil {
			s, v := i.fromSelect.toSQL(len(vars))
			query = fmt.Sprintf("INSERT INTO %s %s", i.Table, s)
			vars = append(vars, v...)
		} else {
			fields := make([]string, 0)
			values := make([]string, 0)

			for j := 0; j < len(i.fields); j++ {
				fields = append(fields, i.fields[j].key)
				values = append(values, i.dialect.Placeholder(j))
			}
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", i.Table, strings.Join(fields, ", "), strings.Join(values, ", "))
		}
	case updateMode:
		updates := make([]string, 0)
		for j, field := range i.fields {
			if field.from == nil {
				updates = append(updates, fmt.Sprintf("%s=%s", field.key, i.dialect.Placeholder(j)))
			} else {
				s, v := field.from.toSQL(len(vars))
				updates = append(updates, fmt.Sprintf("%s=(%s)", field.key, s))
				vars = append(vars, v...)
			}
		}

		query = fmt.Sprintf("UPDATE %s SET %s", i.Table, strings.Join(updates, ", "))
		where, whereVars := i.where.Generate(whereOffset, i.dialect)
		if where != "" {
			query = fmt.Sprintf("%s WHERE %s", query, where)
		}
		vars = append(vars, whereVars...)
	case upsertMode:
		query = i.dialect.MakeUpsert(i.Table, i.conflictColumn, i.fields, 1)
	default:
		panic(fmt.Sprintf("Unknown mode: %#v", i.mode))
	}

	if i.returning != "" {
		query = fmt.Sprintf("%s RETURNING %s", query, i.returning)
	}

	return query, vars
}

func (i *InsertUpdate) HasClauses() bool {
	return len(i.fields) > 0
}

func (i *InsertUpdate) Clauses() map[string]any {
	clauses := make(map[string]any)
	for _, field := range i.fields {
		clauses[field.key] = field.value
	}
	return clauses
}
