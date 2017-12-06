package veryfastprep

import (
	"bufio"
	"bytes"
	"testing"
)

var wordScanTests = [][]byte{
	[]byte(""),
	[]byte(" "),
	[]byte("\n"),
	[]byte("a"),
	[]byte(" a "),
	[]byte("abc def"),
	[]byte(" abc def "),
	[]byte(" abc\tdef\nghi\rjkl\fmno\vpqr\n"),
}

// Test that ASCII byte word splitter returns the same data as strings.Fields.
func TestAsciiSpace(t *testing.T) {
	for n, test := range wordScanTests {
		//given
		buf := bytes.NewReader(test)
		scanner := bufio.NewScanner(buf)
		scanner.Split(ScanWordAsciiSpace)
		expectedWords := bytes.Fields(test)

		var i int
		for i = 0; i < len(expectedWords); i++ {
			if !scanner.Scan() {
				break
			}

			got := scanner.Bytes() //when
			if !bytes.Equal(got, expectedWords[i]) {
				t.Errorf("#%d: %d of %d - expected %q got %q", n, i, len(expectedWords), expectedWords[i], got)
			}
		}
		if scanner.Scan() {
			t.Errorf("#%d: scan too far, got %q", n, scanner.Bytes())
		}
		if i != len(expectedWords) {
			t.Errorf("#%d: stop expected after %d words, but got %d", n, len(expectedWords), i)
		}
		err := scanner.Err()
		if err != nil {
			t.Errorf("#%d: %v", n, err)
		}
	}
}
