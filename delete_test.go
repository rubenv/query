package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	assert := assert.New(t)

	b := NewBuilder(MySQLDialect{})

	s, v := b.Delete("customer", IDEquals(4)).ToSQL()
	assert.Equal(s, "DELETE FROM customer WHERE id=?")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 4)

	s, v = b.Delete("customer", And(
		IDEquals(26),
		FieldEquals("isdeleted", false),
	)).ToSQL()
	assert.Equal(s, "DELETE FROM customer WHERE (id=? AND isdeleted=?)")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], 26)
	assert.Equal(v[1], false)

	s, v = b.Delete("customer", IDEquals(26)).Where(FieldEquals("isdeleted", false)).ToSQL()
	assert.Equal(s, "DELETE FROM customer WHERE id=? AND isdeleted=?")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], 26)
	assert.Equal(v[1], false)

	s, v = b.Delete("customer", And(
		FieldEquals("isdeleted", false),
		Or(
			FieldEquals("firstname", "Jack"),
			FieldEquals("lastname", "Ryan"),
		),
		FieldEquals("status", 1),
	)).ToSQL()
	assert.Equal(s, "DELETE FROM customer WHERE (isdeleted=? AND (firstname=? OR lastname=?) AND status=?)")
	assert.Equal(len(v), 4)
	assert.Equal(v[0], false)
	assert.Equal(v[1], "Jack")
	assert.Equal(v[2], "Ryan")
	assert.Equal(v[3], 1)

	s, v = b.Delete("customer", All()).ToSQL()
	assert.Equal(s, "DELETE FROM customer")
	assert.Equal(len(v), 0)
}

func TestDeleteNum(t *testing.T) {
	assert := assert.New(t)

	b := NewBuilder(PostgreSQLDialect{})

	s, v := b.Delete("customer", IDEquals(4)).ToSQL()
	assert.Equal(s, "DELETE FROM customer WHERE id=$1")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 4)

	s, v = b.Delete("customer", And(
		IDEquals(26),
		FieldEquals("isdeleted", false),
	)).ToSQL()
	assert.Equal(s, "DELETE FROM customer WHERE (id=$1 AND isdeleted=$2)")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], 26)
	assert.Equal(v[1], false)

	s, v = b.Delete("customer", And(
		FieldEquals("isdeleted", false),
		Or(
			FieldEquals("firstname", "Jack"),
			FieldEquals("lastname", "Ryan"),
		),
		FieldEquals("status", 1),
	)).ToSQL()
	assert.Equal(s, "DELETE FROM customer WHERE (isdeleted=$1 AND (firstname=$2 OR lastname=$3) AND status=$4)")
	assert.Equal(len(v), 4)
	assert.Equal(v[0], false)
	assert.Equal(v[1], "Jack")
	assert.Equal(v[2], "Ryan")
	assert.Equal(v[3], 1)

	s, v = b.Delete("customer", All()).ToSQL()
	assert.Equal(s, "DELETE FROM customer")
	assert.Equal(len(v), 0)
}
