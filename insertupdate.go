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
	value interface{}
}

type InsertUpdate struct {
	mode           insertUpdateMode
	Table          string
	where          Where
	fields         []fieldValue
	dialect        Dialect
	conflictColumn []string
}

func (i *InsertUpdate) Add(key string, value interface{}) *InsertUpdate {
	i.fields = append(i.fields, fieldValue{key, value})
	return i
}

func (i *InsertUpdate) With(obj interface{}, opts ...WithOpt) *InsertUpdate {
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
		for j := 0; j < t.NumField(); j++ {
			field := t.Field(j)
			tag := field.Tag.Get("db")
			parts := strings.Split(tag, ",")
			if parts[0] == "" || parts[0] == "-" {
				continue
			}
			if len(parts) > 1 {
				if parts[1] == "autoincrement" && !options.CopyAutoIncrement {
					continue
				}
				if parts[1] == "readonly" && !options.CopyReadOnly {
					continue
				}
			}
			i.Add(parts[0], v.Field(j).Interface())
		}
	}
	return i
}

func (i *InsertUpdate) ToSQL() (string, []interface{}) {
	query := ""
	vars := make([]interface{}, 0)
	for _, v := range i.fields {
		vars = append(vars, v.value)
	}

	switch i.mode {
	case insertMode:
		fields := make([]string, 0)
		values := make([]string, 0)

		for j := 0; j < len(i.fields); j++ {
			fields = append(fields, i.fields[j].key)
			values = append(values, i.dialect.Placeholder(j))
		}
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", i.Table, strings.Join(fields, ", "), strings.Join(values, ", "))
	case updateMode:
		updates := make([]string, 0)
		for j := 0; j < len(i.fields); j++ {
			updates = append(updates, fmt.Sprintf("%s=%s", i.fields[j].key, i.dialect.Placeholder(j)))
		}

		query = fmt.Sprintf("UPDATE %s SET %s", i.Table, strings.Join(updates, ", "))
		where, whereVars := i.where.Generate(len(i.fields), i.dialect)
		if where != "" {
			query = fmt.Sprintf("%s WHERE %s", query, where)
		}
		vars = append(vars, whereVars...)
	case upsertMode:
		query = i.dialect.MakeUpsert(i.Table, i.conflictColumn, i.fields)
	default:
		panic(fmt.Sprintf("Unknown mode: %#v", i.mode))
	}

	return query, vars
}

func (i *InsertUpdate) HasClauses() bool {
	return len(i.fields) > 0
}

func (i *InsertUpdate) Clauses() map[string]interface{} {
	clauses := make(map[string]interface{})
	for _, field := range i.fields {
		clauses[field.key] = field.value
	}
	return clauses
}
