package veryfastprep

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSplit(t *testing.T) {
	testCases := []struct {
		whole int64
		parts int64
		err   bool
		want  []int64
	}{
		{0, 2, true, nil},
		{7, -1, true, nil},
		{7, 9, true, nil},
		{7, 2, false, []int64{4, 3}},
		{5, 4, false, []int64{2, 1, 1, 1}},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d on %d parts", tc.whole, tc.parts), func(t *testing.T) {
			ranges, err := splitRange(tc.whole, tc.parts)
			if tc.err != (err != nil) { // xor
				t.Errorf("got error '%s'; should have error? %t", err, tc.err)
			}

			var got []int64
			for _, r := range ranges {
				got = append(got, r.Size)
			}

			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %s; want %s", got, tc.want)
			}
		})
	}

}
