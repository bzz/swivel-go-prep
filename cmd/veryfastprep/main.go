package main

// A faster implementation of data preprocessing for Swivel
// https://arxiv.org/abs/1602.02215

// There are 3 implementation of data pre-processing required to train a Swivel model
// - Python
//   https://github.com/tensorflow/models/blob/master/research/swivel/prep.py
// - C++
//   https://github.com/tensorflow/models/blob/master/research/swivel/fastprep.cc
// - Scala (Apache Spark)
//   https://github.com/bzz/swivel-spark-prep
//
// This single machine implementation takes advantage of multiple cores to saturate all avialable IO.
// It consits of few stages:
//  - building a vocabulary
//  - building word co-occurence matrix:
//    * sharding co-occurence matrix
//    * sorting each shard on disk
//  - serializing shards to .pb format
//
// Vocabulary is assumed to fit in RAM
// It uses O(1) RAM for building/sorting shards
// Overall it's O(n) of the size of the input

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/bzz/swivel-go-prep"
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
	outpuDir   = flag.String("output_dir", "", "a dir where all output data will be stored")
	shardSize  = flag.String("shard_size", "", "matrix shard size")
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

	fmt.Println("Building vocabulary...")
	vocabStart := time.Now()
	chunks, chunkSize := split(*input, *n)
	wg.Add(len(chunks))
	for i, chunk := range chunks {
		go veryfastprep.BuildVocab(&wg, *input, i, chunkSize, chunk)
	}
	wg.Wait()
	fmt.Println("Done. %s s", time.Since(vocabStart).Seconds())

	fmt.Println("Computing co-occurence matrix shards...")
	shardsStart := time.Now()
	//TODO
	fmt.Println("Done. %s s", time.Since(shardsStart).Seconds())

	fmt.Println("Sorting %d shards...")
	sortStart := time.Now()
	//TODO sort, merge in each shard
	fmt.Println("Done. %s s", time.Since(sortStart).Seconds())

	fmt.Println("Saving %d shards in ProtoBuf...")
	saveStart := time.Now()
	//TODO save to .pb
	fmt.Println("Done. %s s", time.Since(saveStart).Seconds())
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
	fmt.Printf("File:'%s' splitted on %d chunks, %d Mb size, using %d cores\n", fileName, n, chunkSize/veryfastprep.MB, runtime.NumCPU())
	return chunkReaders, chunkSize
}

func newChunkReader(start int64, blockSize int64, fileName string) io.ReadCloser {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("faild to read file %s: %v", fileName, err)
	}

	file.Seek(start, 0)
	l := veryfastprep.LimitReaderCloser(file, blockSize)
	return l
}
