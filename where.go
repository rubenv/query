package query

import (
	"fmt"
	"regexp"
	"strings"
)

type clauseMode int

const (
	emptyClause clauseMode = iota
	andClause
	orClause
	inClause
	notInClause
	likeClause
	ilikeClause
	opClause
	exprClause
	subqueryClause
	nullClause
)

type Where struct {
	mode     clauseMode
	op       string
	field    string
	topLevel bool

	value    interface{}
	values   []interface{}
	children []Where

	subQuery *Select
}

func All() Where {
	return Where{
		mode: emptyClause,
	}
}

// Helper that filters only on ID
func IDEquals(v interface{}) Where {
	return FieldEquals("id", v)
}

func FieldOp(field, op string, value interface{}) Where {
	return Where{
		mode:  opClause,
		op:    op,
		field: field,
		value: value,
	}
}

func FieldEquals(field string, value interface{}) Where {
	return FieldOp(field, "=", value)
}

func FieldNotEquals(field string, value interface{}) Where {
	return FieldOp(field, "!=", value)
}

func IDIn(values []interface{}) Where {
	return FieldIn("id", values)
}

func FieldIn(field string, values []interface{}) Where {
	if len(values) == 0 {
		values = append(values, 0)
	}
	return Where{
		mode:   inClause,
		field:  field,
		values: values,
	}
}

func FieldNotIn(field string, values []interface{}) Where {
	if len(values) == 0 {
		values = append(values, 0)
	}
	return Where{
		mode:   notInClause,
		field:  field,
		values: values,
	}
}

func IntFieldIn(field string, values []int64) Where {
	s := make([]interface{}, len(values))
	for i, v := range values {
		s[i] = v
	}
	return FieldIn(field, s)
}

func StringFieldIn(field string, values []string) Where {
	s := make([]interface{}, len(values))
	for i, v := range values {
		s[i] = v
	}
	return FieldIn(field, s)
}

func Exists(subQuery *Select) Where {
	return Where{
		mode:     subqueryClause,
		op:       "EXISTS",
		subQuery: subQuery,
	}
}

func Any(subQuery *Select) Where {
	return Where{
		mode:     subqueryClause,
		op:       "ANY",
		subQuery: subQuery,
	}
}

func In(field string, subQuery *Select) Where {
	return Where{
		mode:     subqueryClause,
		field:    field,
		op:       "IN",
		subQuery: subQuery,
	}
}

func And(w ...Where) Where {
	return Where{
		mode:     andClause,
		children: w,
	}
}

func Or(w ...Where) Where {
	return Where{
		mode:     orClause,
		children: w,
	}
}

func FieldLike(field string, value interface{}) Where {
	return Where{
		mode:  likeClause,
		field: field,
		value: value,
	}
}

func FieldILike(field string, value interface{}) Where {
	return Where{
		mode:  ilikeClause,
		field: field,
		value: value,
	}
}

func FieldLessThan(field string, value interface{}) Where {
	return FieldOp(field, "<", value)
}

func FieldLessOrEqualThan(field string, value interface{}) Where {
	return FieldOp(field, "<=", value)
}

func FieldGreaterThan(field string, value interface{}) Where {
	return FieldOp(field, ">", value)
}

func FieldGreaterOrEqualThan(field string, value interface{}) Where {
	return FieldOp(field, ">=", value)
}

func Expr(expr string, args ...interface{}) Where {
	return Where{
		mode:   exprClause,
		field:  expr,
		values: args,
	}
}

func IsNull(field string) Where {
	return Where{
		mode:  nullClause,
		field: field,
	}
}

func (w Where) IsEmpty() bool {
	if w.mode == andClause || w.mode == orClause {
		isEmpty := true
		for _, clause := range w.children {
			if !clause.IsEmpty() {
				isEmpty = false
			}
		}
		return isEmpty
	}

	return w.mode == emptyClause
}

var placeholderRe = regexp.MustCompile(`\?+`)

func (w Where) Generate(offset int, dialect Dialect) (string, []interface{}) {
	switch w.mode {
	case emptyClause:
		return "", nil
	case opClause:
		return fmt.Sprintf("%s%s%s", w.field, w.op, dialect.Placeholder(offset)), []interface{}{w.value}
	case andClause:
		return w.generateCompound(offset, "AND", dialect, w.topLevel)
	case orClause:
		return w.generateCompound(offset, "OR", dialect, w.topLevel)
	case inClause:
		placeholders := make([]string, 0)
		for range w.values {
			placeholders = append(placeholders, dialect.Placeholder(offset+len(placeholders)))
		}
		return fmt.Sprintf("%s IN (%s)", w.field, strings.Join(placeholders, ", ")), w.values
	case notInClause:
		placeholders := make([]string, 0)
		for range w.values {
			placeholders = append(placeholders, dialect.Placeholder(offset+len(placeholders)))
		}
		return fmt.Sprintf("%s NOT IN (%s)", w.field, strings.Join(placeholders, ", ")), w.values
	case likeClause:
		return fmt.Sprintf("%s LIKE %s", w.field, dialect.Placeholder(offset)), []interface{}{fmt.Sprintf("%%%s%%", w.value)}
	case ilikeClause:
		return fmt.Sprintf("%s ILIKE %s", w.field, dialect.Placeholder(offset)), []interface{}{fmt.Sprintf("%%%s%%", w.value)}
	case exprClause:
		placeholders := offset
		expr := placeholderRe.ReplaceAllStringFunc(w.field, func(match string) string {
			if match == "??" {
				return "?"
			}
			s := dialect.Placeholder(placeholders)
			placeholders += 1
			return s
		})
		return expr, w.values
	case nullClause:
		return fmt.Sprintf("%s IS NULL", w.field), []interface{}{}
	case subqueryClause:
		f := ""
		if w.field != "" {
			f = fmt.Sprintf("%s ", w.field)
		}
		q, args := w.subQuery.toSQL(offset)
		return fmt.Sprintf("%s%s (%s)", f, w.op, q), args
	default:
		panic(fmt.Sprintf("Unknown mode %#v", w.mode))
	}
}

func (w Where) generateCompound(offset int, verb string, dialect Dialect, topLevel bool) (string, []interface{}) {
	parts := make([]string, 0)
	vars := make([]interface{}, 0)
	for _, clause := range w.children {
		sql, v := clause.Generate(offset, dialect)
		offset += len(v)

		if clause.IsEmpty() {
			continue
		}

		parts = append(parts, sql)
		vars = append(vars, v...)

	}
	prefix, suffix := "", ""
	if !topLevel {
		prefix, suffix = "(", ")"
	}
	switch len(parts) {
	case 0:
		return "", nil
	case 1:
		return parts[0], vars
	default:
		return fmt.Sprintf("%s%s%s", prefix, strings.Join(parts, fmt.Sprintf(" %s ", verb)), suffix), vars
	}
}
