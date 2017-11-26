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
	"log"
	"os"
	"runtime/pprof"
	"time"
)

const wordMaxLength = 10 * KB
const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

func main() {
	var input = flag.String("input", "./data/enwik9", "the input text file")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	size, elapsed := buildDict(input)
	fmt.Printf("Read file:'%s', size:%.2f Mb, time:%.1f sec\n", *input, float64(size)/MB, elapsed.Seconds())
}

// Builds a word dictionary from the given file. Returns a file size.
func buildDict(file *string) (int64, time.Duration) {
	start := time.Now()
	f, err := os.Open(*file)
	if err != nil {
		log.Fatalf("faild to read file %s: %v", file, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1*GB), wordMaxLength)
	scanner.Split(bufio.ScanWords)
	count := 0
	for scanner.Scan() {
		count++
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input:", err)
	}
	fmt.Printf("%d\n", count)

	fi, err := f.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "faild to read file: %s err:%v", file, err)
	}
	elapsed := time.Since(start)
	return fi.Size(), elapsed
}
