package query

type Options struct {
	Where   Where
	Limit   int64
	Offset  int64
	GroupBy string
	OrderBy []string

	Args []interface{}
}

func (o *Options) Merge(opts *Options) *Options {
	if opts != nil {
		if !opts.Where.IsEmpty() {
			o.Where = opts.Where
		}
		if opts.Limit > 0 {
			o.Limit = opts.Limit
		}
		if opts.Offset > 0 {
			o.Offset = opts.Offset
		}
		if len(opts.OrderBy) > 0 {
			o.OrderBy = append(o.OrderBy, opts.OrderBy...)
		}
	}

	return o
}

// Helper that only selects a single ID
func WhereID(v interface{}) *Options {
	return &Options{
		Where: IDEquals(v),
		Limit: 1,
	}
}
