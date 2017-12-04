package main

// A faster implementation of data preprocessing for Swivel.
// https://arxiv.org/abs/1602.02215

// This single machine implementation takes advantage of multiple cores to saturate all avialable IO.
// It consits of few stages:
//  - building a vocabulary
//  - building word co-occurence matrix:
//    * sharding co-occurence matrix
//    * sorting each shard on disk
//  - serializing shards to .pb format
//
// Vocabulary is assumed to fit in RAM (twice).
// It uses O(1) RAM for building/sorting shards.
// Overall it's O(n) of the size of the input.

import (
	"flag"
	"fmt"
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
	inputFileName = flag.String("input", "", "the input text file")
	outpuDir      = flag.String("output_dir", "", "a dir for all output data to be stored")
	shardSize     = flag.Int("shard_size", 4096, "matrix shard size")
	n             = flag.Int64("n", 1, "number of parallel IO threads")
	cpuprofile    = flag.String("cpuprofile", "", "write CPU profile to the file")
	blockprofile  = flag.String("blockprofile", "", "write block profile to the file")
	mutexprofile  = flag.String("mutexprofile", "", "write mutex contention profile to the file")
	wg            sync.WaitGroup
)

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) != 0 || len(*inputFileName) == 0 {
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
	if *blockprofile != "" {
		f, err := os.Create(*blockprofile)
		if err != nil {
			log.Fatal(err)
		}
		runtime.SetBlockProfileRate(1)
		defer pprof.Lookup("block").WriteTo(f, 0)
	}
	if *mutexprofile != "" {
		f, err := os.Create(*mutexprofile)
		if err != nil {
			log.Fatal(err)
		}
		runtime.SetMutexProfileFraction(1)
		defer pprof.Lookup("mutex").WriteTo(f, 0)
	}

	fmt.Println("Building vocabulary...")
	vocabStart := time.Now()
	chunks, err := veryfastprep.Split(*inputFileName, *n)
	if err != nil {
		log.Fatal(err)
	}

	wg.Add(len(chunks))
	out := make(chan *map[string]int64, len(chunks))
	go func() {
		wg.Wait()
		close(out)
	}()

	for i, chunk := range chunks {
		go veryfastprep.BuildVocab(&wg, out, *inputFileName, i, chunk.N, chunk)
	}

	vocabulary := <-out
	for v := range out {
		veryfastprep.MergeVocab(*vocabulary, *v)
	}
	wordToID := veryfastprep.SortVocab(*vocabulary)
	fmt.Printf("Done. %.1f s, size: %d\n", time.Since(vocabStart).Seconds(), len(*vocabulary))

	fmt.Println("Computing co-occurence matrix shards...")
	shardsStart := time.Now()
	_ = wordToID
	//TODO
	// for word in word_window(10, chunk)
	// (wordToID[word], wordToID[word+1], 1)
	fmt.Printf("Done. %.1f s\n", time.Since(shardsStart).Seconds())

	shardsNum := len(*vocabulary) / *shardSize
	fmt.Printf("Sorting %d shards...\n", shardsNum)
	sortStart := time.Now()
	//TODO sort, merge in each shard
	fmt.Printf("Done. %.1f s\n", time.Since(sortStart).Seconds())

	fmt.Printf("Saving %d shards in ProtoBuf...\n", shardsNum)
	saveStart := time.Now()
	//TODO serialize to .pb
	fmt.Printf("Done. %.1f s\n", time.Since(saveStart).Seconds())
}
