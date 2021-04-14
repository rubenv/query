package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWhere(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	id := IDEquals(3)
	s, v := id.Generate(0, MySQLDialect{})
	assert.Equal(s, "id=?")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 3)
}

func TestWhereNum(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	id := IDEquals(3)
	s, v := id.Generate(0, PostgreSQLDialect{})
	assert.Equal(s, "id=$1")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 3)
}

func TestWhereNumOffset(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	id := IDEquals(3)
	s, v := id.Generate(4, PostgreSQLDialect{})
	assert.Equal(s, "id=$5")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 3)
}

func TestWhereIn(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	assert := assert.New(t)

	id := FieldIn("id", []interface{}{4, 5, 6})
	s, v := id.Generate(0, PostgreSQLDialect{})
	assert.Equal(s, "id IN ($1, $2, $3)")
	assert.Equal(len(v), 3)
	assert.Equal(v[0], 4)
	assert.Equal(v[1], 5)
	assert.Equal(v[2], 6)
}

func TestWhereIsNull(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	id := IsNull("id")
	s, v := id.Generate(0, PostgreSQLDialect{})
	assert.Equal(s, "id IS NULL")
	assert.Equal(len(v), 0)
}

func TestWhereAll(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	w := All()
	s, v := w.Generate(0, MySQLDialect{})
	assert.Equal(s, "")
	assert.Equal(len(v), 0)
}

func TestCompoundAndEmpty(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	w := And()
	s, v := w.Generate(0, MySQLDialect{})
	assert.Equal(s, "")
	assert.Equal(len(v), 0)
}

func TestCompoundAndEmptyDeep(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	assert := assert.New(t)

	w := Or()
	s, v := w.Generate(0, MySQLDialect{})
	assert.Equal(s, "")
	assert.Equal(len(v), 0)
}

func TestExpr(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	w := Expr("e.drivers @> jsonb_build_array(jsonb_build_object('id', ?::text, 'name', ?))", 123, "Ruben")
	s, v := w.Generate(1, PostgreSQLDialect{})
	assert.Equal(s, "e.drivers @> jsonb_build_array(jsonb_build_object('id', $2::text, 'name', $3))")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], 123)
	assert.Equal(v[1], "Ruben")

	w = Expr("e.drivers ?? ?", "Ruben")
	s, v = w.Generate(0, PostgreSQLDialect{})
	assert.Equal(s, "e.drivers ? $1")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], "Ruben")
}
