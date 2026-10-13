// Package engine checks the statements an engine's module supplies to the
// outbox and the inbox, which hold no SQL of their own.
package engine

import (
	"fmt"
	"slices"

	"github.com/standards-lab/sqlate/query"
)

// Check reports whether st, the statement an engine supplies for field, is
// defined and takes exactly params, in any order.
func Check(field string, st query.Statement, params ...string) error {
	if st.Name() == "" {
		return fmt.Errorf("%s is not defined", field)
	}
	got := slices.Sorted(slices.Values(st.Params()))
	want := slices.Sorted(slices.Values(params))
	if !slices.Equal(got, want) {
		return fmt.Errorf("%s (%s) takes parameters %v, want %v", field, st.Name(), got, want)
	}
	return nil
}
