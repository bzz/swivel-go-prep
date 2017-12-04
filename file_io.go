package veryfastprep

import (
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
)

// Splits given file on N equal parts.
func Split(file *os.File, n int64) ([]*io.SectionReader, error) {
	if n <= 0 {
		return nil, fmt.Errorf("cann't split '%s' on %d parts", file.Name(), n)
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "faild to get stats for '%s'", file.Name())
	}

	chunks, err := splitRange(fi.Size(), n)
	if err != nil {
		return nil, err
	}

	var readers []*io.SectionReader
	for _, c := range chunks {
		readers = append(readers, io.NewSectionReader(file, c.Start, c.Size))
	}
	fmt.Printf("File '%s': %d chunks, %d Mb\n", file.Name(), n, chunks[0].Size/MB)
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
