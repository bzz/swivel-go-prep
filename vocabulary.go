package veryfastprep

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
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
func BuildVocab(wg *sync.WaitGroup, out chan *map[string]int64, chunkNum int, chunkSize int64, fileChunk *io.SectionReader) {
	fmt.Printf("\t%d - reading size:%.2f Mb\n", chunkNum, float64(chunkSize)/MB)
	defer wg.Done()
	start := time.Now()

	vocab := make(map[string]int64) // TODO(bzz): compare to https://github.com/cornelk/hashmap
	scanner := bufio.NewScanner(fileChunk)
	scanner.Buffer(make([]byte, min(chunkSize, 200*MB)), wordMaxLength)
	scanner.Split(bufio.ScanWords)
	count := 0
	for scanner.Scan() {
		count++
		word := strings.TrimSpace(scanner.Text())
		if word != "" {
			vocab[word]++
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input:", err)
	}

	out <- &vocab
	elapsed := time.Since(start)
	fmt.Printf("\t%d - read time:%.1f sec, words:%d, uniq:%d\n", chunkNum, elapsed.Seconds(), count, len(vocab))
}

// Merge src vocabulary to dst.
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
