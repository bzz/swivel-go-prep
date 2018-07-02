package veryfastprep

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
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

// BuildVocab builds a vocabulary of words from the given file, sorted by frequency.
func BuildVocab(wg *sync.WaitGroup, out chan *map[string]int64, chunkNum int, chunkSize int64, fileChunk *io.SectionReader, bufSize int64) {
	fmt.Printf("\t%d - reading size:%.2f Mb\n", chunkNum, float64(chunkSize)/MB)
	defer wg.Done()
	start := time.Now()

	vocab := make(map[string]int64) // TODO(bzz): compare to https://github.com/cornelk/hashmap
	scanner := bufio.NewScanner(fileChunk)
	scanner.Buffer(make([]byte, min(chunkSize, bufSize*MB)), wordMaxLength)
	scanner.Split(ScanWordAsciiSpace)
	count := 0
	for scanner.Scan() {
		count++
		vocab[scanner.Text()]++
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input:", err)
	}

	out <- &vocab
	elapsed := time.Since(start)
	fmt.Printf("\t%d - read time:%.1f sec, words:%d, uniq:%d\n", chunkNum, elapsed.Seconds(), count, len(vocab))
}

// ScanWordAsciiSpace is a split funciton for a Scanner that returns
// space-separated word of text, with space defined as in
// http://www.cplusplus.com/reference/cctype/isspace
// Analog of bufio.ScanWords but for C sub-set of whitespace chars.
func ScanWordAsciiSpace(data []byte, atEOF bool) (advance int, token []byte, err error) {
	start := 0
	for ; start < len(data); start++ {
		if !isCspace(data[start]) {
			break
		}
	}
	for j := start; j < len(data); j++ {
		if isCspace(data[j]) {
			return j + 1, data[start:j], nil
		}
	}
	// If at EOF, we have a final, non-empty, non-terminated word. Return it.
	if atEOF && len(data) > start {
		return len(data), data[start:], nil
	}
	return start, nil, nil
}

func isCspace(c byte) bool {
	switch c {
	case ' ', '\n', '\r', '\t', '\v', '\f':
		return true
	}
	return false
}

// MergeVocab merges src vocabulary to dst.
func MergeVocab(dst map[string]int64, src map[string]int64) {
	for k, v := range src {
		v2 := dst[k]
		dst[k] = v + v2
	}
}

type Vocab []*KVPair

type KVPair struct {
	K string
	V int64
}

func (v *Vocab) Get(word string) {
	//TODO binary search in []KVPair
}

func (v *Vocab) Print() {
	fmt.Printf("\n\tVocabulary:\n")
	for i, word := range *v {
		fmt.Printf("\t %s - %d\n", word.K, word.V)
		if i > 10 {
			break
		}
	}
	fmt.Printf("Vocab size: %d\n", len(*v))
}

func (v *Vocab) Save(path string) {
	//pick a filename in path
	//save _col
	//save _row
}

func SortVocab(vocab map[string]int64) Vocab {
	var p []*KVPair = make([]*KVPair, len(vocab))
	i := 0
	for k, v := range vocab {
		p[i] = &KVPair{k, v}
		i++
	}
	sort.Slice(p, func(i, j int) bool { return p[i].V > p[j].V })
	return p
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
