package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

func usage() {
	usageMessage := `usage: read -input <fileName>
Read a single file as fast as possible
`
	fmt.Fprintf(os.Stderr, usageMessage)
	flag.PrintDefaults()
	os.Exit(2)
}

var (
	file       = flag.String("file", "", "the input text file")
	block      = flag.Int("block", 100, "size of the block in Mb")
	cpuprofile = flag.String("cpuprofile", "", "write CPU profile to the file")
)

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) != 0 || len(*file) == 0 {
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

	f, err := os.Open(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "faild to read file: %s err:%v", file, err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, *block*MB)
	mem := make([]byte, *block*MB)

	var total float64 = 0
	count := 0
	for {
		start := time.Now()
		if n, err := r.Read(mem); err == nil || err == io.EOF {
			took := time.Since(start).Seconds()
			//fmt.Printf("\t%dMb read, took %.2fs - %.2f Mb/s \n", n/MB, took, float64(n)/MB/took)
			count += n
			total += took
			if err == io.EOF {
				break
			}
		} else {
			log.Fatal(err)
		}
	}

	fmt.Printf("%d Mb total, avg: %.2f Mb/sec\n", count/MB, float64(count)/MB/total)
}
