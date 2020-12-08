package query

type InsertUpdateOptions struct {
	CopyAutoIncrement bool
	CopyReadOnly      bool
}

type WithOpt func(o *InsertUpdateOptions)

func WithAutoIncrement() WithOpt {
	return func(o *InsertUpdateOptions) {
		o.CopyAutoIncrement = true
	}
}

func WithReadOnly() WithOpt {
	return func(o *InsertUpdateOptions) {
		o.CopyReadOnly = true
	}
}
