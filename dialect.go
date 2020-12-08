package query

import (
	"fmt"
	"strings"
)

type Dialect interface {
	Placeholder(idx int) string
	UseLastInsertId() bool
	MakeUpsert(table string, conflictColumn []string, fields []fieldValue) string
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

func (d MySQLDialect) MakeUpsert(table string, conflictColumn []string, fields []fieldValue) string {
	fieldNames := make([]string, 0)
	values := make([]string, 0)

	for j := 0; j < len(fields); j++ {
		fieldNames = append(fieldNames, fields[j].key)
		values = append(values, d.Placeholder(j))
	}
	return fmt.Sprintf("REPLACE INTO %s (%s) VALUES (%s)", table, strings.Join(fieldNames, ", "), strings.Join(values, ", "))
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

func (d SqliteDialect) MakeUpsert(table string, conflictColumn []string, fields []fieldValue) string {
	return postgreSQLUpsert(table, conflictColumn, fields)
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

func (d PostgreSQLDialect) MakeUpsert(table string, conflictColumn []string, fields []fieldValue) string {
	return postgreSQLUpsert(table, conflictColumn, fields)
}

// Shared functionality
func numberedPlaceholder(idx int) string {
	return fmt.Sprintf("$%d", idx+1)
}

func postgreSQLUpsert(table string, conflictColumn []string, fields []fieldValue) string {
	fieldNames := make([]string, 0)
	values := make([]string, 0)
	updates := make([]string, 0)

	for j := 0; j < len(fields); j++ {
		fieldNames = append(fieldNames, fields[j].key)
		values = append(values, numberedPlaceholder(j))
		updates = append(updates, fmt.Sprintf("%s=EXCLUDED.%s", fields[j].key, fields[j].key))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s", table, strings.Join(fieldNames, ", "), strings.Join(values, ", "), strings.Join(conflictColumn, ", "), strings.Join(updates, ", "))
}
