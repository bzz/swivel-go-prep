package veryfastprep

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"

	"github.com/pkg/errors"
)

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

// Splits given file on N equal parts.
func Split(fileName string, n int64) ([]io.ReadCloser, int64, error) {
	if n <= 0 {
		return nil, 0, fmt.Errorf("cann't split '%s' on %d parts", fileName, n)
	}

	file, err := os.Open(fileName)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "faild to read file '%s'", fileName)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return nil, 0, errors.Wrapf(err, "faild to get stats for '%s'", file.Name())
	}

	var readers []io.ReadCloser
	chunks, err := splitRange(fi.Size(), n)
	if err != nil {
		return nil, 0, err
	}

	var chunkSize int64
	for _, c := range chunks {
		readers = append(readers, newChunkReader(c.Start, c.Size, file.Name()))
		chunkSize = c.Size
	}
	fmt.Printf("File '%s': %d chunks, %d Mb, using %d cores\n", file.Name(), n, chunkSize/MB, runtime.NumCPU())
	return readers, chunkSize, nil
}

type Range struct {
	Start int64
	Size  int64
}

func splitRange(fileSize int64, n int64) ([]*Range, error) {
	if n > fileSize || fileSize <= 0 {
		return nil, fmt.Errorf("cann't split size:%d on %d parts", fileSize, n)
	}
	chunkSize := fileSize / n
	first := chunkSize + fileSize%chunkSize
	sum := first

	chunks := make([]*Range, n)
	//fmt.Printf("%d %d %d %d\n", fileSize, first,s 0, 0)
	chunks[0] = &Range{0, first}
	for i := first; i < fileSize; i += chunkSize {
		//fmt.Printf("%d %d %d %d\n", fileSize, chunkSize, i/chunkSize, i)
		chunks[i/chunkSize] = &Range{i, chunkSize}
		sum += chunkSize
	}
	if sum != fileSize {
		return nil, errors.New("chunk split does not cover whole file")
	}
	return chunks, nil
}

func newChunkReader(start int64, blockSize int64, fileName string) io.ReadCloser {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("faild to read file %s: %v", fileName, err)
	}

	file.Seek(start, 0)
	l := LimitReaderCloser(file, blockSize)
	return l
}
