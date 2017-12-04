package veryfastprep

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"

	"github.com/pkg/errors"
)

// io.LimitedReader that can be closed.
type LimitedReaderCloser struct {
	R io.ReadCloser
	N int64
}

// https://golang.org/pkg/io/#LimitedReader.Read
func (l *LimitedReaderCloser) Read(p []byte) (n int, err error) {
	fmt.Printf("\t\tReading %d Mb\n", len(p)/MB)
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
func Split(fileName string, n int64) ([]*LimitedReaderCloser, error) {
	if n <= 0 {
		return nil, fmt.Errorf("cann't split '%s' on %d parts", fileName, n)
	}

	file, err := os.Open(fileName)
	if err != nil {
		return nil, errors.Wrapf(err, "faild to read file '%s'", fileName)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "faild to get stats for '%s'", file.Name())
	}

	chunks, err := splitRange(fi.Size(), n)
	if err != nil {
		return nil, err
	}

	var readers []*LimitedReaderCloser
	for _, c := range chunks {
		readers = append(readers, newChunkReader(c.Start, c.Size, file.Name()))
	}
	fmt.Printf("File '%s': %d chunks, %d Mb, using %d cores\n", file.Name(), n, chunks[0].Size/MB, runtime.NumCPU())
	return readers, nil
}

type Range struct {
	Start int64
	Size  int64
}

func splitRange(fileSize int64, n int64) ([]*Range, error) {
	if n > fileSize || fileSize <= 0 || n <= 0 {
		return nil, fmt.Errorf("cann't split size:%d on %d parts", fileSize, n)
	}
	chunkSize := fileSize / n
	firstSize := chunkSize + fileSize%n
	sum := firstSize

	chunks := make([]*Range, n)
	fmt.Printf("%d %d %d %d\n", fileSize, firstSize, 0, 0)
	chunks[0] = &Range{0, firstSize}
	for i := firstSize - 1; i < fileSize-1; i += chunkSize {
		fmt.Printf("%d %d %d %d\n", fileSize, chunkSize, i/chunkSize, i)
		chunks[i/chunkSize] = &Range{i, chunkSize}
		sum += chunkSize
	}
	if sum != fileSize {
		return nil, errors.New("chunk split does not cover whole file")
	}
	return chunks, nil
}

func newChunkReader(start int64, blockSize int64, fileName string) *LimitedReaderCloser {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("faild to open file %s: %v", fileName, err)
	}
	fmt.Printf("\tOpen file %s at %d\n", fileName, start)
	ret, err := file.Seek(start, 0)
	if err != nil || ret != start {
		log.Fatalf("faild to seek in file %s to %d: %v", fileName, start, err)
	}
	return &LimitedReaderCloser{R: file, N: blockSize}
}
