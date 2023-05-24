package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	t.Parallel()

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

	sq, sv := b.Select("*", "contacts").Where(FieldNotEquals("age", 5)).ToSQL()
	assert.Equal(sq, "SELECT * FROM contacts WHERE age!=?")
	assert.Equal(len(sv), 1)
	assert.Equal(sv[0], 5)

	s, v = b.Select("count(c.*)", fmt.Sprintf("(%s) c", sq), sv...).Where(FieldGreaterThan("c.age", 25)).ToSQL()
	assert.Equal(s, "SELECT count(c.*) FROM (SELECT * FROM contacts WHERE age!=?) c WHERE c.age>?")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], 5)
	assert.Equal(v[1], 25)
}

func TestQueryNum(t *testing.T) {
	t.Parallel()

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

	s, v = b.Select("*", "contacts").
		Where(Exists(b.Select("*", "places").Where(FieldEquals("country", "BE")))).
		Where(Any(b.Select("*", "countries").Where(FieldEquals("region", "EU")))).
		Where(FieldEquals("activated", true)).
		ToSQL()
	assert.Equal(s, "SELECT * FROM contacts WHERE EXISTS (SELECT * FROM places WHERE country=$1) AND ANY (SELECT * FROM countries WHERE region=$2) AND activated=$3")
	assert.Equal(len(v), 3)
	assert.Equal(v[0], "BE")
	assert.Equal(v[1], "EU")
	assert.Equal(v[2], true)

	sq, sv := b.Select("*", "contacts").Where(FieldNotEquals("age", 5)).ToSQL()
	assert.Equal(sq, "SELECT * FROM contacts WHERE age!=$1")
	assert.Equal(len(sv), 1)
	assert.Equal(sv[0], 5)

	s, v = b.Select("count(c.*)", fmt.Sprintf("(%s) c", sq), sv...).Where(FieldGreaterThan("c.age", 25)).ToSQL()
	assert.Equal(s, "SELECT count(c.*) FROM (SELECT * FROM contacts WHERE age!=$1) c WHERE c.age>$2")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], 5)
	assert.Equal(v[1], 25)

	s, v = b.Select("count(*), name", "people").GroupBy("name").Having(FieldGreaterThan("count(*)", 5)).ToSQL()
	assert.Equal(s, "SELECT count(*), name FROM people GROUP BY name HAVING count(*)>$1")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], 5)

	s, v = b.Select("*", "test").Where(In("team", b.Select("id", "teams").Where(IDEquals(3)))).Where(IDEquals(2)).ToSQL()
	assert.Equal(s, "SELECT * FROM test WHERE team IN (SELECT id FROM teams WHERE id=$1) AND id=$2")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], 3)
	assert.Equal(v[1], 2)
}

func TestMerge(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	o := (&Options{
		Where: FieldEquals("test", 123),
		Limit: 10,
	}).Merge(&Options{
		Offset:  20,
		Limit:   15,
		OrderBy: []string{"name DESC"},
	})

	assert.Equal(o, &Options{
		Where:   FieldEquals("test", 123),
		Offset:  20,
		Limit:   15,
		OrderBy: []string{"name DESC"},
	})
}

func TestGroupBy(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	b := NewBuilder(PostgreSQLDialect{})

	s, v := b.Select("sum(age)", "contacts").GroupBy("gender").ToSQL()
	assert.Equal(s, "SELECT sum(age) FROM contacts GROUP BY gender")
	assert.Equal(len(v), 0)
}

func TestUnion(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	b := NewBuilder(PostgreSQLDialect{})

	s, v := b.Select("*", "contacts").Union(b.Select("*", "archived_contacts")).ToSQL()
	assert.Equal(s, "SELECT * FROM contacts UNION SELECT * FROM archived_contacts")
	assert.Equal(len(v), 0)
}

func TestCTE(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	b := NewBuilder(PostgreSQLDialect{})

	s, v := b.Select("hour, sum(count) over (order by hour asc rows between unbounded preceding and current row)", "data").
		With(With{
			Name:      "data",
			SubSelect: b.Select("date_trunc('hour', created_at) as hour, count(1)", "orders").Where(FieldEquals("user", 226)).GroupBy("1"),
		}).
		With(With{
			Name:      "other",
			SubSelect: b.Select("*", "test").Where(FieldEquals("field", "a")),
		}).
		Where(FieldEquals("x", 3)).
		ToSQL()

	assert.Len(v, 3)
	assert.Equal(v[0], 226)
	assert.Equal(v[1], "a")
	assert.Equal(v[2], 3)

	assert.Equal(`WITH
    data AS (SELECT date_trunc('hour', created_at) as hour, count(1) FROM orders WHERE user=$1 GROUP BY 1),
    other AS (SELECT * FROM test WHERE field=$2)
SELECT hour, sum(count) over (order by hour asc rows between unbounded preceding and current row) FROM data WHERE x=$3`, s)
}
