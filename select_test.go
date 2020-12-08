package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	assert := assert.New(t)

	b := NewBuilder(MySQLDialect{})

	s, v := b.Select("*", "contacts").ToSQL()
	assert.Equal(s, "SELECT * FROM contacts")
	assert.Equal(len(v), 0)

	s, v = b.Select("*", "contacts").Limit(10).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts LIMIT 10")
	assert.Equal(len(v), 0)

	s, v = b.Select("*", "contacts").Limit(10).Offset(20).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts LIMIT 10 OFFSET 20")
	assert.Equal(len(v), 0)

	s, v = b.Select("*", "contacts").Where(IDEquals(123)).Limit(10).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE id=? LIMIT 10")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 123)

	s, v = b.Select("*", "contacts").Where(IDEquals(123)).Limit(10).OrderByDesc("name").ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE id=? ORDER BY name DESC LIMIT 10")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 123)

	s, v = b.Select("*", "contacts").Where(FieldLike("lower(name)", "angels")).Limit(10).OrderByDesc("name").ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE lower(name) LIKE ? ORDER BY name DESC LIMIT 10")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], "%angels%")

	s, v = b.Select("*", "contacts").Where(FieldLessThan("age", 12)).Limit(10).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE age<? LIMIT 10")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 12)

	s, v = b.Select("*", "contacts").Where(FieldGreaterThan("age", 25)).Limit(10).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE age>? LIMIT 10")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 25)

	s, v = b.Select("*", "contacts").Where(FieldNotEquals("age", 5)).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE age!=?")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 5)

	s, v = b.Select("*", "contacts").Where(And()).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts")
	assert.Equal(len(v), 0)
}

func TestQueryNum(t *testing.T) {
	assert := assert.New(t)

	b := NewBuilder(PostgreSQLDialect{})

	s, v := b.Select("*", "contacts").ToSQL()
	assert.Equal(s, "SELECT * FROM contacts")
	assert.Equal(len(v), 0)

	s, v = b.Select("*", "contacts").Limit(10).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts LIMIT 10")
	assert.Equal(len(v), 0)

	s, v = b.Select("*", "contacts").Limit(10).Offset(20).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts LIMIT 10 OFFSET 20")
	assert.Equal(len(v), 0)

	s, v = b.Select("*", "contacts").Where(IDEquals(123)).Limit(10).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE id=$1 LIMIT 10")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 123)

	s, v = b.Select("*", "contacts").Where(IDEquals(123)).Limit(10).OrderByDesc("name").ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE id=$1 ORDER BY name DESC LIMIT 10")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 123)

	s, v = b.Select("*", "contacts").Where(FieldLike("lower(name)", "angels")).Limit(10).OrderByDesc("name").ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE lower(name) LIKE $1 ORDER BY name DESC LIMIT 10")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], "%angels%")

	s, v = b.Select("*", "contacts").Where(FieldLessThan("age", 12)).Limit(10).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE age<$1 LIMIT 10")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 12)

	s, v = b.Select("*", "contacts").Where(FieldGreaterThan("age", 25)).Limit(10).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE age>$1 LIMIT 10")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 25)

	s, v = b.Select("*", "contacts").Where(FieldNotEquals("age", 5)).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE age!=$1")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 5)

	s, v = b.Select("*", "contacts").Where(FieldNotEquals("age", 5)).Where(FieldEquals("known", true)).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE age!=$1 AND known=$2")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], 5)
	assert.Equal(v[1], true)

	s, v = b.Select("*", "contacts").Where(And()).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts")
	assert.Equal(len(v), 0)

	s, v = b.Select("c.*", "contacts c").
		LeftJoin("addresses a", Expr("a.contact=c.id")).
		Join("organizations o", And(Expr("c.organization=o.id"), FieldEquals("o.deleted", false))).
		Where(FieldEquals("c.activated", true)).
		ToSQL()
	assert.Equal(s, "SELECT c.* FROM contacts c LEFT JOIN addresses a ON a.contact=c.id INNER JOIN organizations o ON (c.organization=o.id AND o.deleted=$1) WHERE c.activated=$2")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], false)
	assert.Equal(v[1], true)
}

func TestMerge(t *testing.T) {
	assert := assert.New(t)

	o := (&Options{
		Where: FieldEquals("test", 123),
		Limit: 10,
	}).Merge(&Options{
		Offset:  20,
		Limit:   15,
		OrderBy: OrderByDesc("name"),
	})

	assert.Equal(o, &Options{
		Where:   FieldEquals("test", 123),
		Offset:  20,
		Limit:   15,
		OrderBy: OrderByDesc("name"),
	})
}

func TestGroupBy(t *testing.T) {
	assert := assert.New(t)

	b := NewBuilder(PostgreSQLDialect{})

	s, v := b.Select("sum(age)", "contacts").GroupBy("gender").ToSQL()
	assert.Equal(s, "SELECT sum(age) FROM contacts GROUP BY gender")
	assert.Equal(len(v), 0)
}
