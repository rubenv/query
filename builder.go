package query

type Builder struct {
	dialect Dialect
}

func NewBuilder(dialect Dialect) *Builder {
	return &Builder{
		dialect: dialect,
	}
}

func (b *Builder) Select(fields, table string, args ...interface{}) *Select {
	return &Select{
		Dialect: b.dialect,
		Fields:  fields,
		Table:   table,
		Args:    args,
	}
}

func (b *Builder) BulkInsert(table string, columns []string) *BulkInsert {
	return &BulkInsert{
		mode:    insertMode,
		Table:   table,
		Dialect: b.dialect,
		Columns: columns,
	}
}

func (b *Builder) BulkUpsert(table string, columns []string, conflictColumn []string) *BulkInsert {
	return &BulkInsert{
		mode:           upsertMode,
		Table:          table,
		Dialect:        b.dialect,
		Columns:        columns,
		conflictColumn: conflictColumn,
	}
}

// Explicitly takes a WHERE, since you'll almost always do this.
//
// You can always pass All() in case you want to truncate the table, but at
// least that way it's obvious.
func (b *Builder) Delete(table string, where Where) *Delete {
	d := &Delete{
		Table:   table,
		Dialect: b.dialect,
	}
	d.Where(where)
	return d
}

func (b *Builder) Insert(table string) *InsertUpdate {
	return &InsertUpdate{
		Table:   table,
		mode:    insertMode,
		dialect: b.dialect,
	}
}

func (b *Builder) Update(table string, where Where) *InsertUpdate {
	return &InsertUpdate{
		mode:    updateMode,
		Table:   table,
		where:   where,
		dialect: b.dialect,
	}
}

func (b *Builder) Upsert(table string, conflictColumn ...string) *InsertUpdate {
	return &InsertUpdate{
		mode:           upsertMode,
		Table:          table,
		dialect:        b.dialect,
		conflictColumn: conflictColumn,
	}
}
