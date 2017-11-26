package veryfastprep

import "io"

// io.LimiteddReader that can be closed.
type LimitedReaderCloser struct {
	R io.ReadCloser
	N int64
}

func LimitReaderCloser(r io.ReadCloser, n int64) io.ReadCloser {
	return &LimitedReaderCloser{R: r, N: n}
}

// https://golang.org/pkg/io/#LimitedReader.Read
func (l *LimitedReaderCloser) Read(p []byte) (n int, err error) {
	if l.N <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > l.N {
		p = p[0:l.N]
	}
	n, err = l.R.Read(p)
	l.N -= int64(n)
	return
}

func (l *LimitedReaderCloser) Close() error {
	return l.R.Close()
}
