package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderBy(t *testing.T) {
	assert := assert.New(t)

	o := OrderBy("name")
	s := o.generate()
	assert.Equal(o.IsEmpty(), false)
	assert.Equal(s, "name")

	o = OrderByDesc("name")
	s = o.generate()
	assert.Equal(s, "name DESC")
}
