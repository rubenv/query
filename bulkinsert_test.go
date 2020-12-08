package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBulkInsert(t *testing.T) {
	assert := assert.New(t)

	b := NewBuilder(PostgreSQLDialect{})

	insert := b.BulkInsert("customers", []string{
		"id",
		"name",
		"country",
	})

	assert.NoError(insert.Add(123, "Test", "BE"))
	assert.NoError(insert.Add(456, "Test 2", "NL"))
	assert.NoError(insert.Add(789, "Test 3", "FR"))

	query, args := insert.ToSQL()
	assert.Equal(query, "INSERT INTO customers (id, name, country) VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)")
	assert.Len(args, 9)
	assert.Equal(args[0], 123)
	assert.Equal(args[1], "Test")
	assert.Equal(args[2], "BE")
	assert.Equal(args[3], 456)
	assert.Equal(args[4], "Test 2")
	assert.Equal(args[5], "NL")
	assert.Equal(args[6], 789)
	assert.Equal(args[7], "Test 3")
	assert.Equal(args[8], "FR")
}
