package query

import (
	"fmt"
	"strings"
)

type Dialect interface {
	Placeholder(idx int) string
	UseLastInsertId() bool
	MakeUpsert(table string, conflictColumn []string, fields []fieldValue, rows int) string
}

func DialectFromString(dialect string) (Dialect, error) {
	switch dialect {
	case "mysql":
		return MySQLDialect{}, nil
	case "postgres":
		return PostgreSQLDialect{}, nil
	case "sqlite3":
		return SqliteDialect{}, nil
	default:
		return nil, fmt.Errorf("Unknown dialect: %s", dialect)
	}
}

// Generates queries using question marks
type MySQLDialect struct {
}

func (d MySQLDialect) Placeholder(idx int) string {
	return "?"
}

func (d MySQLDialect) UseLastInsertId() bool {
	return true
}

func (d MySQLDialect) MakeUpsert(table string, conflictColumn []string, fields []fieldValue, rows int) string {
	fieldNames := make([]string, 0)
	placeholders := make([]string, 0)

	for _, fn := range fields {
		fieldNames = append(fieldNames, fn.key)
	}

	placeholderVars := make([]string, 0)
	for range fields {
		placeholderVars = append(placeholderVars, "?")
	}
	placeholder := fmt.Sprintf("(%s)", strings.Join(placeholderVars, ", "))
	for i := 0; i < rows; i++ {
		placeholders = append(placeholders, placeholder)
	}

	return fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s", table, strings.Join(fieldNames, ", "), strings.Join(placeholders, ", "))
}

// Generates queries using numbered placeholders
type SqliteDialect struct {
}

func (d SqliteDialect) Placeholder(idx int) string {
	return numberedPlaceholder(idx)
}

func (d SqliteDialect) UseLastInsertId() bool {
	return true
}

func (d SqliteDialect) MakeUpsert(table string, conflictColumn []string, fields []fieldValue, rows int) string {
	return postgreSQLUpsert(table, conflictColumn, fields, rows)
}

// Generates queries using numbered placeholders
type PostgreSQLDialect struct {
}

func (d PostgreSQLDialect) Placeholder(idx int) string {
	return numberedPlaceholder(idx)
}

func (d PostgreSQLDialect) UseLastInsertId() bool {
	return false
}

func (d PostgreSQLDialect) MakeUpsert(table string, conflictColumn []string, fields []fieldValue, rows int) string {
	return postgreSQLUpsert(table, conflictColumn, fields, rows)
}

// Shared functionality
func numberedPlaceholder(idx int) string {
	return fmt.Sprintf("$%d", idx+1)
}

func postgreSQLUpsert(table string, conflictColumn []string, fields []fieldValue, rows int) string {
	fieldNames := make([]string, 0)
	placeholders := make([]string, 0)

	for _, fn := range fields {
		fieldNames = append(fieldNames, fn.key)
	}

	for i := 0; i < rows; i++ {
		placeholderVars := make([]string, 0)
		for j := range fields {
			placeholderVars = append(placeholderVars, numberedPlaceholder(i*len(fields)+j))
		}
		placeholder := fmt.Sprintf("(%s)", strings.Join(placeholderVars, ", "))
		placeholders = append(placeholders, placeholder)
	}
	conflictCol := ""
	if len(conflictColumn) > 0 {
		conflictCol = fmt.Sprintf(" (%s)", strings.Join(conflictColumn, ", "))
	}
	action := "NOTHING"
	if len(conflictColumn) > 0 {
		updates := make([]string, 0)
		for _, fn := range fieldNames {
			updates = append(updates, fmt.Sprintf("%s=EXCLUDED.%s", fn, fn))
		}
		action = fmt.Sprintf("UPDATE SET %s", strings.Join(updates, ", "))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT%s DO %s", table, strings.Join(fieldNames, ", "), strings.Join(placeholders, ", "), conflictCol, action)
}
