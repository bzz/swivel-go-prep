package veryfastprep

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const wordMaxLength = 10 * KB
const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

// Builds a vocabulary of words from the given file, sorted by frequency.
func BuildVocab(wg *sync.WaitGroup, fileName string, chunkNum int, chunkSize int64, fileChunk io.ReadCloser) {
	fmt.Printf("\t%d - reading file:'%s', size:%.2f Mb\n", chunkNum, fileName, float64(chunkSize)/MB)
	defer wg.Done()
	defer fileChunk.Close()
	start := time.Now()

	scanner := bufio.NewScanner(fileChunk)
	scanner.Buffer(make([]byte, min(chunkSize, 100*MB)), wordMaxLength)
	scanner.Split(bufio.ScanWords)
	count := 0
	for scanner.Scan() {
		count++
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input:", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("\t%d - read time:%.1f sec, words:%d\n", chunkNum, elapsed.Seconds(), count)
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
