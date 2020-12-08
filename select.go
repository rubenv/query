package query

import (
	"fmt"
	"strconv"
	"strings"
)

type Select struct {
	Dialect Dialect
	Fields  string
	Table   string
	Options Options
	Joins   []Join
}

type Join struct {
	Join  string
	Table string
	On    Where
}

func (s *Select) Where(where Where) *Select {
	if s.Options.Where.IsEmpty() {
		s.Options.Where = And()
		s.Options.Where.topLevel = true
	}
	if s.Options.Where.mode != andClause {
		panic("Cannot chain Where() if you've set a manual where clause")
	}
	s.Options.Where.children = append(s.Options.Where.children, where)
	return s
}

func (s *Select) GroupBy(field string) *Select {
	s.Options.GroupBy = field
	return s
}

func (s *Select) Limit(limit int64) *Select {
	s.Options.Limit = limit
	return s
}

func (s *Select) Offset(offset int64) *Select {
	s.Options.Offset = offset
	return s
}

func (s *Select) OrderBy(field string) *Select {
	s.Options.OrderBy = OrderBy(field)
	return s
}

func (s *Select) OrderByDesc(field string) *Select {
	s.Options.OrderBy = OrderByDesc(field)
	return s
}

func (s *Select) OrderByDir(field string, desc bool) *Select {
	s.Options.OrderBy = OrderByDir(field, desc)
	return s
}

func (s *Select) Join(table string, on Where) *Select {
	s.Joins = append(s.Joins, Join{
		Join:  "INNER",
		Table: table,
		On:    on,
	})
	return s
}

func (s *Select) LeftJoin(table string, on Where) *Select {
	s.Joins = append(s.Joins, Join{
		Join:  "LEFT",
		Table: table,
		On:    on,
	})
	return s
}

func (s *Select) ToSQL() (string, []interface{}) {
	return s.toSQL(0)
}

func (s *Select) toSQL(offset int) (string, []interface{}) {
	b := strings.Builder{}
	args := make([]interface{}, 0)
	b.WriteString(fmt.Sprintf("SELECT %s FROM %s", s.Fields, s.Table))
	for _, join := range s.Joins {
		b.WriteString(" ")
		b.WriteString(join.Join)
		b.WriteString(" JOIN ")
		b.WriteString(join.Table)
		b.WriteString(" ON ")
		q, v := join.On.Generate(offset+len(args), s.Dialect)
		b.WriteString(q)
		args = append(args, v...)
	}
	if !s.Options.Where.IsEmpty() {
		q, v := s.Options.Where.Generate(offset+len(args), s.Dialect)
		if len(q) > 0 {
			b.WriteString(" WHERE ")
			b.WriteString(q)
			args = append(args, v...)
		}
	}
	if s.Options.GroupBy != "" {
		b.WriteString(" GROUP BY ")
		b.WriteString(s.Options.GroupBy)
	}
	if !s.Options.OrderBy.IsEmpty() {
		o := s.Options.OrderBy.generate()
		b.WriteString(" ORDER BY ")
		b.WriteString(o)
	}
	if s.Options.Limit > 0 {
		b.WriteString(" LIMIT ")
		b.WriteString(strconv.FormatInt(s.Options.Limit, 10))
	}
	if s.Options.Offset > 0 {
		b.WriteString(" OFFSET ")
		b.WriteString(strconv.FormatInt(s.Options.Offset, 10))
	}
	return b.String(), args
}
