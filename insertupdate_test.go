package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	b := NewBuilder(MySQLDialect{})

	insert := b.Insert("customer")
	insert.Add("firstname", "Jack")
	insert.Add("age", 23)

	s, v := insert.ToSQL()
	assert.Equal(s, "INSERT INTO customer (firstname, age) VALUES (?, ?)")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], "Jack")
	assert.Equal(v[1], 23)

	s, v = b.Insert("customer").
		Add("firstname", "Jack").
		Add("age", 23).
		ToSQL()
	assert.Equal(s, "INSERT INTO customer (firstname, age) VALUES (?, ?)")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], "Jack")
	assert.Equal(v[1], 23)

	type VAT struct {
		Nr int64 `db:"nr"`
	}

	type Company struct {
		ID   int64  `db:"id,autoincrement"`
		Name string `db:"name"`
		VAT
	}

	type Customer struct {
		Firstname string `db:"firstname"`
		Age       int64  `db:"age"`
		Ignore    string
		Ignore2   string  `db:"-"`
		Company   Company `db:"company,readonly"`
		Position  string  `db:"position,text"`
	}

	s, v = b.Insert("customer").
		With(&Customer{
			Firstname: "Jack",
			Age:       23,
			Ignore:    "nope",
			Ignore2:   "nope2",
			Position:  "xxx",
		}).
		ToSQL()
	assert.Equal(s, "INSERT INTO customer (firstname, age, position) VALUES (?, ?, ?)")
	assert.Equal(len(v), 3)
	assert.Equal(v[0], "Jack")
	assert.Equal(v[1], int64(23))
	assert.Equal(v[2], "xxx")

	s, v = b.Insert("company").
		With(&Company{
			Name: "Corp",
			VAT: VAT{
				Nr: 1234,
			},
		}).
		ToSQL()
	assert.Equal(s, "INSERT INTO company (name, nr) VALUES (?, ?)")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], "Corp")
	assert.Equal(v[1], int64(1234))

	s, v = b.Insert("company").
		With(&Company{
			ID:   123,
			Name: "Corp",
			VAT: VAT{
				Nr: 1234,
			},
		}).
		Returning("id").
		ToSQL()
	assert.Equal(s, "INSERT INTO company (id, name, nr) VALUES (?, ?, ?) RETURNING id")
	assert.Equal(len(v), 3)
	assert.Equal(v[0], int64(123))
	assert.Equal(v[1], "Corp")
	assert.Equal(v[2], int64(1234))
}

func TestInsertNum(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	b := NewBuilder(PostgreSQLDialect{})

	insert := b.Insert("customer")
	insert.Add("firstname", "Jack")
	insert.Add("age", 23)

	s, v := insert.ToSQL()
	assert.Equal(s, "INSERT INTO customer (firstname, age) VALUES ($1, $2)")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], "Jack")
	assert.Equal(v[1], 23)

	s, v = b.Insert("customer").
		Add("firstname", "Jack").
		Add("age", 23).
		ToSQL()
	assert.Equal(s, "INSERT INTO customer (firstname, age) VALUES ($1, $2)")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], "Jack")
	assert.Equal(v[1], 23)

	type Company struct {
	}

	type Customer struct {
		Firstname string `db:"firstname"`
		Age       int64  `db:"age"`
		Ignore    string
		Ignore2   string  `db:"-"`
		Company   Company `db:"company,readonly"`
	}

	s, v = b.Insert("customer").
		With(&Customer{
			Firstname: "Jack",
			Age:       23,
			Ignore:    "nope",
			Ignore2:   "nope2",
		}).
		ToSQL()
	assert.Equal(s, "INSERT INTO customer (firstname, age) VALUES ($1, $2)")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], "Jack")
	assert.Equal(v[1], int64(23))

	s, v = b.Insert("customer").
		With(&Customer{
			Firstname: "Jack",
			Age:       23,
			Ignore:    "nope",
			Ignore2:   "nope2",
		}, WithReadOnly()).
		ToSQL()
	assert.Equal(s, "INSERT INTO customer (firstname, age, company) VALUES ($1, $2, $3)")
	assert.Equal(len(v), 3)
	assert.Equal(v[0], "Jack")
	assert.Equal(v[1], int64(23))
	assert.Equal(v[2], Company{})
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	b := NewBuilder(MySQLDialect{})

	update := b.Update("customer", IDEquals(4))
	update.Add("firstname", "Jack")
	update.Add("age", 23)

	s, v := update.ToSQL()
	assert.Equal(s, "UPDATE customer SET firstname=?, age=? WHERE id=?")
	assert.Equal(len(v), 3)
	assert.Equal(v[0], "Jack")
	assert.Equal(v[1], 23)
	assert.Equal(v[2], 4)

	update = b.Update("customer", And(
		IDEquals(26),
		FieldEquals("isdeleted", false),
	))
	update.Add("firstname", "Bob")

	s, v = update.ToSQL()
	assert.Equal(s, "UPDATE customer SET firstname=? WHERE (id=? AND isdeleted=?)")
	assert.Equal(len(v), 3)
	assert.Equal(v[0], "Bob")
	assert.Equal(v[1], 26)
	assert.Equal(v[2], false)

	update = b.Update("customer", And(
		FieldEquals("isdeleted", false),
		Or(
			FieldEquals("firstname", "Jack"),
			FieldEquals("lastname", "Ryan"),
		),
		FieldEquals("status", 1),
	))
	update.Add("firstname", "Bob")
	update.Add("lastname", "Doe")

	s, v = update.ToSQL()
	assert.Equal(s, "UPDATE customer SET firstname=?, lastname=? WHERE (isdeleted=? AND (firstname=? OR lastname=?) AND status=?)")
	assert.Equal(len(v), 6)
	assert.Equal(v[0], "Bob")
	assert.Equal(v[1], "Doe")
	assert.Equal(v[2], false)
	assert.Equal(v[3], "Jack")
	assert.Equal(v[4], "Ryan")
	assert.Equal(v[5], 1)

	update = b.Update("customer", All())
	update.Add("firstname", "Bob")
	s, v = update.ToSQL()
	assert.Equal(s, "UPDATE customer SET firstname=?")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], "Bob")

	update = b.Update("customer", And())
	update.Add("firstname", "Bob")

	s, v = update.ToSQL()
	assert.Equal(s, "UPDATE customer SET firstname=?")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], "Bob")
}

func TestUpdateNum(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	b := NewBuilder(PostgreSQLDialect{})

	s, v := b.Update("customer", IDEquals(4)).
		Add("firstname", "Jack").
		Add("age", 23).ToSQL()
	assert.Equal(s, "UPDATE customer SET firstname=$1, age=$2 WHERE id=$3")
	assert.Equal(len(v), 3)
	assert.Equal(v[0], "Jack")
	assert.Equal(v[1], 23)
	assert.Equal(v[2], 4)

	s, v = b.Update("customer", And(
		IDEquals(26),
		FieldEquals("isdeleted", false),
	)).Add("firstname", "Bob").ToSQL()
	assert.Equal(s, "UPDATE customer SET firstname=$1 WHERE (id=$2 AND isdeleted=$3)")
	assert.Equal(len(v), 3)
	assert.Equal(v[0], "Bob")
	assert.Equal(v[1], 26)
	assert.Equal(v[2], false)

	s, v = b.Update("customer", And(
		FieldEquals("isdeleted", false),
		Or(
			FieldEquals("firstname", "Jack"),
			FieldEquals("lastname", "Ryan"),
		),
		FieldEquals("status", 1),
	)).
		Add("firstname", "Bob").
		Add("lastname", "Doe").
		ToSQL()
	assert.Equal(s, "UPDATE customer SET firstname=$1, lastname=$2 WHERE (isdeleted=$3 AND (firstname=$4 OR lastname=$5) AND status=$6)")
	assert.Equal(len(v), 6)
	assert.Equal(v[0], "Bob")
	assert.Equal(v[1], "Doe")
	assert.Equal(v[2], false)
	assert.Equal(v[3], "Jack")
	assert.Equal(v[4], "Ryan")
	assert.Equal(v[5], 1)

	s, v = b.Update("customer", All()).Add("firstname", "Bob").ToSQL()
	assert.Equal(s, "UPDATE customer SET firstname=$1")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], "Bob")

	s, v = b.Update("customer", And()).Add("firstname", "Bob").ToSQL()
	assert.Equal(s, "UPDATE customer SET firstname=$1")
	assert.Equal(len(v), 1)
	assert.Equal(v[0], "Bob")

	update := b.Update("customer", Expr("id=?", 4))
	update.Add("firstname", "Jack")

	s, v = update.ToSQL()
	assert.Equal(s, "UPDATE customer SET firstname=$1 WHERE id=$2")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], "Jack")
	assert.Equal(v[1], 4)
}

func TestUpsert(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	b := NewBuilder(PostgreSQLDialect{})

	upsert := b.Upsert("customer", "id")
	upsert.Add("firstname", "Jack")
	upsert.Add("age", 23)

	s, v := upsert.ToSQL()
	assert.Equal(s, "INSERT INTO customer (firstname, age) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET firstname=EXCLUDED.firstname, age=EXCLUDED.age")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], "Jack")
	assert.Equal(v[1], 23)
}

func TestUpsertNoCol(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	b := NewBuilder(PostgreSQLDialect{})

	upsert := b.Upsert("customer")
	upsert.Add("firstname", "Jack")
	upsert.Add("age", 23)

	s, v := upsert.ToSQL()
	assert.Equal(s, "INSERT INTO customer (firstname, age) VALUES ($1, $2) ON CONFLICT DO NOTHING")
	assert.Equal(len(v), 2)
	assert.Equal(v[0], "Jack")
	assert.Equal(v[1], 23)
}

func TestUpsertMySQL(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	b := NewBuilder(MySQLDialect{})

	upsert := b.Upsert("customer", "id")
	upsert.Add("id", 123)
	upsert.Add("firstname", "Jack")
	upsert.Add("age", 23)

	s, v := upsert.ToSQL()
	assert.Equal(s, "REPLACE INTO customer (id, firstname, age) VALUES (?, ?, ?)")
	assert.Equal(len(v), 3)
	assert.Equal(v[0], 123)
	assert.Equal(v[1], "Jack")
	assert.Equal(v[2], 23)
}
