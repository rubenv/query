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
	Unions  []*Select
	CTEs    []With
	Args    []any
}

type With struct {
	Name      string
	SubSelect *Select
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

func (s *Select) Having(where Where) *Select {
	if s.Options.Having.IsEmpty() {
		s.Options.Having = And()
		s.Options.Having.topLevel = true
	}
	if s.Options.Having.mode != andClause {
		panic("Cannot chain Having() if you've set a manual having clause")
	}
	s.Options.Having.children = append(s.Options.Having.children, where)
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

func (s *Select) OrderBy(fields ...string) *Select {
	s.Options.OrderBy = append(s.Options.OrderBy, fields...)
	return s
}

func (s *Select) OrderByDesc(field string) *Select {
	return s.OrderBy(fmt.Sprintf("%s DESC", field))
}

func (s *Select) OrderByDir(field string, desc bool) *Select {
	if desc {
		return s.OrderByDesc(field)
	} else {
		return s.OrderBy(field)
	}
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

func (s *Select) Union(o *Select) *Select {
	s.Unions = append(s.Unions, o)
	return s
}

func (s *Select) With(o With) *Select {
	s.CTEs = append(s.CTEs, o)
	return s
}

func (s *Select) ToSQL() (string, []any) {
	return s.toSQL(0)
}

func (s *Select) ToSQLArgs(existingArgs []any) (string, []any) {
	q, args := s.toSQL(len(existingArgs))
	return q, append(existingArgs, args...)
}

func (s *Select) toSQL(offset int) (string, []any) {
	b := strings.Builder{}
	args := make([]any, 0)

	if len(s.CTEs) > 0 {
		b.WriteString("WITH\n")
		for i, w := range s.CTEs {
			if i > 0 {
				b.WriteString(",\n")
			}
			q, a := w.SubSelect.toSQL(len(args))
			b.WriteString(fmt.Sprintf("    %s AS (%s)", w.Name, q))
			args = append(args, a...)
		}
		b.WriteString("\n")
	}

	args = append(args, s.Args...)
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
	if !s.Options.Having.IsEmpty() {
		q, v := s.Options.Having.Generate(offset+len(args), s.Dialect)
		if len(q) > 0 {
			b.WriteString(" HAVING ")
			b.WriteString(q)
			args = append(args, v...)
		}
	}
	for _, u := range s.Unions {
		q, v := u.toSQL(offset + len(args))
		b.WriteString(" UNION ")
		b.WriteString(q)
		args = append(args, v...)
	}
	if len(s.Options.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		b.WriteString(strings.Join(s.Options.OrderBy, ", "))
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
