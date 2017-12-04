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
func BuildVocab(wg *sync.WaitGroup, out chan *map[string]int64, fileName string, chunkNum int, chunkSize int64, fileChunk io.ReadCloser) {
	fmt.Printf("\t%d - reading file:'%s', size:%.2f Mb\n", chunkNum, fileName, float64(chunkSize)/MB)
	defer wg.Done()
	defer fileChunk.Close()
	start := time.Now()

	vocab := make(map[string]int64) // TODO(bzz): compare to https://github.com/cornelk/hashmap
	scanner := bufio.NewScanner(fileChunk)
	scanner.Buffer(make([]byte, min(chunkSize, 200*MB)), wordMaxLength)
	scanner.Split(bufio.ScanWords)
	count := 0
	for scanner.Scan() {
		count++
		word := scanner.Text()
		val := vocab[word]
		vocab[word] = val + 1
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input:", err)
	}

	out <- &vocab
	elapsed := time.Since(start)
	fmt.Printf("\t%d - read time:%.1f sec, words:%d\n", chunkNum, elapsed.Seconds(), count)
}

// Merge src vocabulary to dst.
func MergeVocab(dst map[string]int64, src map[string]int64) {
	for k, v := range src {
		v2 := dst[k]
		dst[k] = v + v2
	}
}

type Vocab []KVPair

type KVPair struct {
	k string
	v int64
}

func (*Vocab) Get(word string) {
	//TODO binary search in []KVPair
}

func SortVocab(vocab map[string]int64) Vocab {
	var pairs = make([]KVPair, len(vocab))
	for k, v := range vocab {
		pairs = append(pairs, KVPair{k, v})
	}
	//TODO sort pairs by v
  //sort.Slice(p, func(i, j int) bool { return p[i].Name < p[j].Name })
	return pairs
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
