package main

// A faster implementation of data preprocessing for Swivel
// https://arxiv.org/abs/1602.02215

// There are 3 implementation of data pre-processing required to train a Swivel model
// - Python
// - C++
// - Scala (Apache Spark)
//
// This implementation takes advantage of multiple cores to saturate all avialable IO.
// It consits of few stages:
//  - building a vocabulary
//  - computing word co-occurence stats
//  - sharding co-occurence matrix and sorting each shard on disk
//  - serializing shards to .pb format
//
// Vocabulary is assumed to fit in RAM
// It uses O(1) RAM for building/sorting shards
// Overall it's O(n) of the size of the input

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
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

var usageMessage = `usage: veryfastprepgo [-n] [-input]
Build word co-occurence matrix for Swivel.
For more details see https://github.com/tensorflow/models/tree/master/research/swivel
`

func usage() {
	fmt.Fprintf(os.Stderr, usageMessage)
	flag.PrintDefaults()
	os.Exit(2)
}

var (
	input      = flag.String("input", "", "the input text file")
	n          = flag.Int64("n", 1, "number of parallel IO threads")
	cpuprofile = flag.String("cpuprofile", "", "write CPU profile to this file")
	wg         sync.WaitGroup
)

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) != 0 || len(*input) == 0 {
		flag.Usage()
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	chunks, chunkSize := split(*input, *n)
	fmt.Printf("File:'%s' splitted on %d chunks, %d Mb size, using %d cores\n", *input, len(chunks), chunkSize/MB, runtime.NumCPU())
	wg.Add(len(chunks))
	for i, chunk := range chunks {
		go buildDict(&wg, *input, i, chunkSize, chunk)
	}
	wg.Wait()
}

// Splits given file on N equal parts.
func split(fileName string, n int64) ([]io.ReadCloser, int64) {
	if n <= 0 {
		log.Fatalf("cann't split '%s' on %d parts", fileName, n)
	}

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("faild to read file %s: %v", fileName, err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		log.Fatalf("faild to read file %s: %v", file.Name(), err)
	}

	fileSize := fi.Size()
	if n > fileSize {
		log.Fatalf("cann't split '%s' size:%d on %d parts", file.Name(), fileSize, n)
	}
	chunkSize := fileSize / n
	first := chunkSize + fileSize%chunkSize
	sum := first

	chunkReaders := make([]io.ReadCloser, n)
	//fmt.Printf("%d %d %d %d\n", fileSize, first, 0, 0)
	chunkReaders[0] = newChunkReader(0, first, file.Name())
	for i := first; i < fileSize; i += chunkSize {
		//fmt.Printf("%d %d %d %d\n", fileSize, chunkSize, i/chunkSize, i)
		chunkReaders[i/chunkSize] = newChunkReader(i, chunkSize, file.Name())
		sum += chunkSize
	}
	if sum != fileSize {
		log.Fatalf("chunk split does not cover whole file")
	}
	return chunkReaders, chunkSize
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

// Builds a word dictionary from the given file
func buildDict(wg *sync.WaitGroup, fileName string, chunkNum int, chunkSize int64, fileChunk io.ReadCloser) {
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
