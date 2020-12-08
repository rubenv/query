package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWhere(t *testing.T) {
	assert := assert.New(t)

	id := IDEquals(3)
	s, v := id.Generate(0, MySQLDialect{})
	assert.Equal(s, "id=?")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 3)
}

func TestWhereNum(t *testing.T) {
	assert := assert.New(t)

	id := IDEquals(3)
	s, v := id.Generate(0, PostgreSQLDialect{})
	assert.Equal(s, "id=$1")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 3)
}

func TestWhereNumOffset(t *testing.T) {
	assert := assert.New(t)

	id := IDEquals(3)
	s, v := id.Generate(4, PostgreSQLDialect{})
	assert.Equal(s, "id=$5")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 3)
}

func TestWhereIn(t *testing.T) {
	assert := assert.New(t)

	id := FieldIn("id", []interface{}{4, 5, 6})
	s, v := id.Generate(0, MySQLDialect{})
	assert.Equal(s, "id IN (?, ?, ?)")
	assert.Equal(len(v), 3)
	assert.Equal(v[0], 4)
	assert.Equal(v[1], 5)
	assert.Equal(v[2], 6)
}

func TestWhereInNum(t *testing.T) {
	assert := assert.New(t)

	id := FieldIn("id", []interface{}{4, 5, 6})
	s, v := id.Generate(0, PostgreSQLDialect{})
	assert.Equal(s, "id IN ($1, $2, $3)")
	assert.Equal(len(v), 3)
	assert.Equal(v[0], 4)
	assert.Equal(v[1], 5)
	assert.Equal(v[2], 6)
}

func TestWhereAll(t *testing.T) {
	assert := assert.New(t)

	w := All()
	s, v := w.Generate(0, MySQLDialect{})
	assert.Equal(s, "")
	assert.Equal(len(v), 0)
}

func TestCompoundAndEmpty(t *testing.T) {
	assert := assert.New(t)

	w := And()
	s, v := w.Generate(0, MySQLDialect{})
	assert.Equal(s, "")
	assert.Equal(len(v), 0)
}

func TestCompoundAndEmptyDeep(t *testing.T) {
	assert := assert.New(t)

	w := And(
		FieldEquals("id", 123),
		And(),
	)
	s, v := w.Generate(0, MySQLDialect{})
	assert.Equal(s, "id=?")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 123)
}

func TestCompoundAndAll(t *testing.T) {
	assert := assert.New(t)

	w := And(
		FieldEquals("id", 2),
		All(),
	)
	s, v := w.Generate(0, MySQLDialect{})
	assert.Equal(s, "id=?")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 2)

	w = And(
		All(),
		All(),
	)
	s, v = w.Generate(0, MySQLDialect{})
	assert.Equal(s, "")
	assert.Equal(len(v), 0)

	w = And(
		All(),
	)
	s, v = w.Generate(0, MySQLDialect{})
	assert.Equal(s, "")
	assert.Equal(len(v), 0)
}

func TestCompoundOrEmpty(t *testing.T) {
	assert := assert.New(t)

	w := Or()
	s, v := w.Generate(0, MySQLDialect{})
	assert.Equal(s, "")
	assert.Equal(len(v), 0)
}
